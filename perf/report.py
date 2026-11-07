#!/usr/bin/env python3
"""Combine k6 summary, docker stats, backlog samples and per-step SQL latency into results.json and summary.md (called by perf/run.sh)."""
import argparse, csv, json, re, statistics as st, sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("out"); ap.add_argument("--mode"); ap.add_argument("--drain"); ap.add_argument("--violations"); ap.add_argument("--k6"); ap.add_argument("--k6-exit")
ap.add_argument("--step-seconds", type=float, default=30); ap.add_argument("--warmup", type=float, default=60); ap.add_argument("--duration", type=float, default=180)
a = ap.parse_args(); out = Path(a.out)

k6 = json.loads((out / "k6-summary.json").read_text())
e2e = {r["step"]: r for r in json.loads((out / "e2e.json").read_text() or "[]")}
start = k6["scenario_start_ms"] / 1000.0

def mem_mib(s):
    m = re.match(r"([0-9.]+)(KiB|MiB|GiB)", s); v = float(m.group(1))
    return v / 1024 if m.group(2) == "KiB" else v * 1024 if m.group(2) == "GiB" else v
stats = []   # (epoch, container, cpu%, mem MiB)
for r in csv.reader(open(out / "stats.csv")):
    if len(r) >= 4 and r[0].isdigit():
        name = re.search(r"((?:payment|ledger)-service(?:-[a-z])?|postgres|kafka|prometheus|grafana|jaeger|lb)-(\d+)$", r[1])
        stats.append((int(r[0]), f"{name.group(1)}-{name.group(2)}" if name else r[1], float(r[2].rstrip("%")), mem_mib(r[3].split(" / ")[0])))
backlog = [(int(r[0]), int(r[1])) for r in csv.reader(open(out / "backlog.csv")) if len(r) == 2 and r[0].isdigit() and r[1].lstrip("-").isdigit()]

def cpu_window(t0, t1):
    by = {}
    for e, name, cpu, mem in stats:
        if t0 <= e <= t1: by.setdefault(name, []).append((cpu, mem))
    return {n: {"cpu_median": round(st.median(c for c, _ in v), 1), "cpu_max": round(max(c for c, _ in v), 1),
                "mem_median_mib": round(st.median(m for _, m in v)), "mem_max_mib": round(max(m for _, m in v)), "samples": len(v)} for n, v in sorted(by.items())}

def backlog_window(t0, t1):
    v = [b for e, b in backlog if t0 <= e <= t1]
    return {"max": max(v), "end": v[-1]} if v else None

rows = []
t = start
for s in k6["steps"]:
    t0 = start + (0 if a.mode == "steady" and s["step"] == 0 else (a.warmup if a.mode == "steady" else (s["step"] - 1) * a.step_seconds))
    t1 = t0 + s["seconds"]
    ok = s["accepted"] + s["declined"]; fails = s["http_4xx"] + s["http_5xx"] + s["no_response"]
    db = e2e.get(s["step"], {})
    rows.append({**s, "achieved": round(ok / s["seconds"], 1), "failed": fails, "e2e_p50": db.get("e2e_p50"), "e2e_p95": db.get("e2e_p95"), "e2e_p99": db.get("e2e_p99"),
                 "payments_in_db": db.get("payments"), "pending_after_drain": db.get("pending"), "completed": db.get("completed"), "failed_terminal": db.get("failed_terminal"),
                 "window": [round(t0), round(t1)], "cpu": cpu_window(t0, t1), "backlog": backlog_window(t0, t1)})

res = {"mode": a.mode, "k6": a.k6, "k6_exit": int(a.k6_exit), "dropped_iterations": k6["dropped_iterations"], "vus_max": k6["vus_max"],
       "drain_s": a.drain, "invariant_violations": a.violations, "steps": rows}
if a.mode == "ramp":
    # a step is "saturated" when achieved < 95 % of offered, or any failed request, or POST p95 > 1 s, or the backlog at its end > 200
    def saturated(r): return (r["achieved"] < 0.95 * r["offered"] or r["failed"] > 0 or r["p95"] > 1000 or (r["backlog"] and r["backlog"]["end"] > 200))
    first = next((r for r in rows if saturated(r)), None)
    good = [r for r in rows if not saturated(r) and (first is None or r["step"] < first["step"])]
    res["first_saturated_step"] = first["step"] if first else None
    res["first_saturated_offered"] = first["offered"] if first else None
    res["last_sustained_offered"] = good[-1]["offered"] if good else None
    res["max_achieved"] = max(r["achieved"] for r in rows)
    res["steady_target_70pct"] = round(0.7 * good[-1]["offered"]) if good else None
(out / "results.json").write_text(json.dumps(res, indent=1))

L = [f"mode {a.mode}; load generator {a.k6} (exit {a.k6_exit}); dropped iterations {k6['dropped_iterations']}; max VUs {k6['vus_max']}; drain {a.drain} s; invariant violations {a.violations}"]
if a.mode == "ramp":
    L += ["| step | offered/s | achieved/s | failed | POST p50/p95/p99 ms | e2e p50/p95/p99 ms | backlog max/end | payment CPU % med | ledger CPU % med | postgres CPU % med |", "|---|---|---|---|---|---|---|---|---|---|"]
    cm = lambda r, svc: round(sum(v["cpu_median"] for n, v in r["cpu"].items() if n.startswith(svc)), 1) or "-"   # summed over replicas
    for r in rows:
        b = r["backlog"] or {}
        L.append(f"| {r['step']} | {r['offered']} | {r['achieved']} | {r['failed']} | {r['p50']:.1f}/{r['p95']:.1f}/{r['p99']:.1f} | {r['e2e_p50']}/{r['e2e_p95']}/{r['e2e_p99']} | {b.get('max','-')}/{b.get('end','-')} | {cm(r,'payment-service')} | {cm(r,'ledger-service')} | {cm(r,'postgres')} |")
    L.append(f"first saturated step: {res['first_saturated_step']} (offered {res['first_saturated_offered']}/s); last sustained offered: {res['last_sustained_offered']}/s; max achieved: {res['max_achieved']}/s; 70 % steady target: {res['steady_target_70pct']}/s")
else:
    r = next(x for x in rows if x["step"] == 1)
    L += [f"measured window {a.duration:.0f} s after a {a.warmup:.0f} s warm-up (excluded)",
          f"offered {r['offered']}/s, achieved {r['achieved']}/s, accepted {r['accepted']}, declined {r['declined']}, failed {r['failed']} (4xx {r['http_4xx']}, 5xx {r['http_5xx']}, no response {r['no_response']})",
          f"POST p50/p95/p99 ms: {r['p50']:.1f}/{r['p95']:.1f}/{r['p99']:.1f}   request-to-terminal p50/p95/p99 ms: {r['e2e_p50']}/{r['e2e_p95']}/{r['e2e_p99']}   pending after drain: {r['pending_after_drain']}"]
    for n, v in r["cpu"].items(): L.append(f"  {n}: CPU median {v['cpu_median']} % max {v['cpu_max']} %, memory median {v['mem_median_mib']} MiB max {v['mem_max_mib']} MiB")
    if r["backlog"]: L.append(f"  backlog max {r['backlog']['max']} end {r['backlog']['end']}")
(out / "summary.md").write_text("\n".join(L) + "\n")
print("\n".join(L))
