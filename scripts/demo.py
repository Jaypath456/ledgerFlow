#!/usr/bin/env python3
"""Fresh isolated Compose demonstration. Keeps all containers, volumes and evidence."""
import argparse
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from evidence import sanitize_text

ROOT = Path(__file__).resolve().parents[1]


class DemoCheckError(RuntimeError):
    """A demonstration did not produce its required outcome."""


class EnvironmentCollisionError(DemoCheckError):
    """An existing environment must not be reused or changed."""


class FinancialInvariantError(DemoCheckError):
    """A balance, posting, or full-history financial check failed."""


def require(condition, message):
    if not condition:
        raise DemoCheckError(message)


class Demo:
    def __init__(self, output):
        self.project = os.environ.get("PROJECT", "ledgerflow-delivery-" + uuid.uuid4().hex[:10])
        if not self.project.startswith("ledgerflow-delivery-"):
            raise ValueError("PROJECT must start with ledgerflow-delivery-; existing projects are never reused")
        self.output = Path(output or ROOT / "chaos/results" / self.project).resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.env = dict(os.environ, PAYMENT_PORT=os.environ.get("PAYMENT_PORT", "18081"),
                        LEDGER_PORT=os.environ.get("LEDGER_PORT", "18082"),
                        MONGO_PORT=os.environ.get("MONGO_PORT", "37017"),
                        KAFKA_PORT=os.environ.get("KAFKA_PORT", "39092"),
                        ALICE_ACCOUNTS="100001,100003,100004,100005,100006,100007,100008",
                        BOB_ACCOUNTS="100002", RISK_VELOCITY_MAX="5", PAYMENT_PROFILES="default",
                        LEDGER_PROFILES="default", PAYMENT_CHAOS_FAULTS="", LEDGER_CHAOS_FAULTS="")
        self.compose = ["docker", "compose", "-p", self.project, "-f", str(ROOT / "infra/docker-compose.yml"),
                        "-f", str(ROOT / "infra/docker-compose.delivery.yml")]
        self.base = "http://127.0.0.1:" + self.env["PAYMENT_PORT"]
        self.results = []
        self.command_number = 0
        self.started = False
        self.paused = False

    def command(self, args, data=None, check=True):
        self.command_number += 1
        with (self.output / "commands.jsonl").open("a") as log:
            log.write(sanitize_text(json.dumps(args)) + "\n")
        result = subprocess.run(args, cwd=ROOT, env=self.env, input=data, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=360)
        safe_output = sanitize_text(result.stdout)
        (self.output / f"command-{self.command_number:03d}.log").write_text(safe_output)
        if check and result.returncode:
            raise RuntimeError(sanitize_text(f"Command failed ({result.returncode}): {args}: {safe_output[-2000:]}"))
        return result.stdout.strip()

    def c(self, *args, **kwargs):
        return self.command(self.compose + list(args), **kwargs)

    def mongo(self, script):
        return self.c("exec", "-T", "mongo", "mongosh", "admin", "--quiet", "-u", "local_admin", "-p", "local_admin",
                      "--file", "/dev/stdin", data=script)

    def query(self, expression):
        return json.loads(self.mongo("print(JSON.stringify(" + expression + "));"))

    def http(self, method, path, body=None, key=None, identity="alice", timeout=45):
        headers = {"Content-Type": "application/json"}
        if identity:
            password = self.env.get(identity.upper() + "_PASSWORD", identity + "-local")
            headers["Authorization"] = "Basic " + base64.b64encode((identity + ":" + password).encode()).decode()
        if key:
            headers["Idempotency-Key"] = key
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data, headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.read(), response.headers

    def pay(self, payer, amount=100, key=None, identity="alice"):
        return self.http("POST", "/api/payments", dict(payerAccountId=payer, payeeAccountId=200001,
                         amountMinor=amount, currency="USD"), key or str(uuid.uuid4()), identity)

    def terminal(self, response, identity="alice"):
        require(response[0] in (201, 202), f"Expected payment admission response, got {response[:2]}")
        payment_id = json.loads(response[1])["id"]
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            status, body, _ = self.http("GET", "/api/payments/" + payment_id, identity=identity)
            require(status == 200, f"Payment read returned {status}: {body}")
            payment = json.loads(body)
            if payment["status"] != "PENDING_LEDGER":
                return payment
            time.sleep(.2)
        raise TimeoutError("Payment did not converge: " + payment_id)

    def wait_health(self):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                if self.http("GET", "/actuator/health", identity=None, timeout=3)[0] == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(1)
        raise TimeoutError("Payment service failed to recover")

    def check(self, name, details):
        deadline = time.monotonic() + 60
        while self.query("db.getSiblingDB('payments').outbox.countDocuments({published_at:null})"
                         "+db.getSiblingDB('ledger').outbox.countDocuments({published_at:null})"):
            if time.monotonic() >= deadline:
                raise TimeoutError("Outboxes did not drain")
            time.sleep(.2)
        violations = self.mongo((ROOT / "chaos/verify_invariants.js").read_text())
        if violations:
            raise FinancialInvariantError(violations)
        self.results.append(dict(scenario=name, result="PASS", violations=0, details=details))
        (self.output / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")
        print("PASS:", name, json.dumps(details), "I1-I6 violations=0", flush=True)

    def effects(self, payment, payer, expected_balance):
        result = self.query("({postings:db.getSiblingDB('ledger').ledger_transactions.countDocuments({payment_id:"
                            + json.dumps(payment["id"]) + "}),balance:Number(db.getSiblingDB('ledger').accounts.findOne({_id:Long('"
                            + str(payer) + "')}).balance_minor)})")
        expected = dict(postings=1 if payment["status"] == "COMPLETED" else 0, balance=expected_balance)
        if result != expected:
            raise FinancialInvariantError(f"Expected {expected}, got {result}")
        return result

    def start(self):
        containers = self.c("ps", "-a", "-q")
        if containers:
            raise EnvironmentCollisionError("Refusing to reuse existing containers")
        existing = self.command(["docker", "volume", "ls", "--format", "{{.Name}}"])
        if self.project + "_mongodata" in existing.splitlines():
            raise EnvironmentCollisionError("Refusing to reuse an existing volume")
        for key in ("PAYMENT_PORT", "LEDGER_PORT", "MONGO_PORT", "KAFKA_PORT"):
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", int(self.env[key])))
        (self.output / "environment.json").write_text(json.dumps({k: self.env[k] for k in (
            "PAYMENT_PORT", "LEDGER_PORT", "MONGO_PORT", "KAFKA_PORT", "ALICE_ACCOUNTS", "BOB_ACCOUNTS", "RISK_VELOCITY_MAX")}, indent=2))
        print("Starting", self.project, "evidence:", self.output, flush=True)
        self.started = True
        self.c("up", "-d", "--build", "--wait", "--wait-timeout", "240")
        create = (ROOT / "chaos/create_accounts.js").read_text()
        for first, count, kind, amount in ((100001, 1, "CUSTOMER", 5000), (100002, 1, "CUSTOMER", 100),
                                          (100003, 6, "CUSTOMER", 10000), (200001, 1, "MERCHANT", 0)):
            self.mongo(f"const FIRST={first},COUNT={count},TYPE='{kind}',AMOUNT={amount};\n" + create)

    def basic(self):
        key = str(uuid.uuid4())
        first = self.pay(100001, 1500, key)
        done = self.terminal(first)
        require(done["status"] == "COMPLETED", f"Successful payment did not complete: {done}")
        self.check("successful payment", self.effects(done, 100001, 3500))
        declined = self.terminal(self.pay(100001, 1000001))
        require(declined["status"] == "DECLINED" and declined["declineReason"] == "AMOUNT_LIMIT",
                f"Unexpected risk outcome: {declined}")
        self.check("risk-declined payment", self.effects(declined, 100001, 3500))
        failed = self.terminal(self.pay(100002, 500, identity="bob"), "bob")
        require(failed["status"] == "FAILED" and failed["declineReason"] == "INSUFFICIENT_FUNDS",
                f"Unexpected insufficient-funds outcome: {failed}")
        self.check("insufficient funds", self.effects(failed, 100002, 100))
        replay = self.pay(100001, 1500, key)
        require(replay[:2] == first[:2] and replay[2]["Idempotent-Replayed"] == "true", "Replay changed the stored response")
        conflict = self.pay(100001, 1501, key)
        require(conflict[0] == 422, f"Expected payload conflict, got HTTP {conflict[0]}")
        self.check("idempotent replay and payload conflict", self.effects(done, 100001, 3500))
        before = self.query("db.getSiblingDB('payments').payments.countDocuments({})")
        unauthorized = self.pay(100001, identity=None)
        require(unauthorized[0] == 401, f"Unauthenticated request returned HTTP {unauthorized[0]}")
        unowned = self.pay(100001, identity="bob")
        require(unowned[0] == 403, f"Unowned debit returned HTTP {unowned[0]}")
        after = self.query("db.getSiblingDB('payments').payments.countDocuments({})")
        require(after == before, "Rejected authorization probes changed payment state")
        self.check("unauthorized and unowned debit rejected", dict(unauthenticated=401, unowned=403))
        barrier = threading.Barrier(32)
        def hot(_):
            barrier.wait(timeout=20)
            return self.pay(100003)
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            rows = list(pool.map(hot, range(32)))
        require(sum(row[0] == 202 for row in rows) == 5, "Velocity burst did not admit exactly 5 payments")
        require(sum(row[0] == 201 for row in rows) == 27, "Velocity burst did not decline exactly 27 payments")
        payments = [self.terminal(row) for row in rows]
        for row, payment in zip(rows, payments):
            require(payment["status"] == ("COMPLETED" if row[0] == 202 else "DECLINED"),
                    f"Unexpected velocity payment outcome: {payment}")
            self.effects(payment, 100003, 9500)
        self.check("concurrent velocity limit", dict(requests=32, completed=5, declined=27, balance=9500))
        self.c("stop", "ledger-service")
        try:
            accepted = self.pay(100004)
            require(accepted[0] == 202, f"Expected acceptance during ledger outage, got HTTP {accepted[0]}")
            self.effects(json.loads(accepted[1]), 100004, 10000)
        finally:
            self.c("start", "ledger-service")
        recovered = self.terminal(accepted)
        require(recovered["status"] == "COMPLETED", "Payment did not complete after ledger restart")
        self.check("ledger service interruption", self.effects(recovered, 100004, 9900))

    def extended(self):
        self.c("stop", "ledger-service")
        key = str(uuid.uuid4())
        try:
            accepted = self.pay(100005, key=key)
            require(accepted[0] == 202, f"Expected acceptance before payment restart, got HTTP {accepted[0]}")
            self.c("kill", "-s", "SIGKILL", "payment-service")
            self.c("start", "payment-service")
            self.wait_health()
            replay = self.pay(100005, key=key)
            require(replay[:2] == accepted[:2] and replay[2]["Idempotent-Replayed"] == "true",
                    "Replay changed after payment restart")
        finally:
            self.c("start", "ledger-service")
        payment = self.terminal(accepted)
        require(payment["status"] == "COMPLETED", "Payment did not complete after process kill")
        self.check("payment process killed after acceptance", self.effects(payment, 100005, 9900))
        self.c("pause", "mongo")
        self.paused = True
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.pay, 100006)
            try:
                time.sleep(3)
                require(not pending.done(), "Expected submission to wait while MongoDB is paused")
            finally:
                self.c("unpause", "mongo")
                self.paused = False
            response = pending.result(timeout=60)
        payment = self.terminal(response)
        require(payment["status"] == "COMPLETED", "Payment did not complete after MongoDB resumed")
        self.check("MongoDB paused during submission", self.effects(payment, 100006, 9900))
        self.c("stop", "kafka")
        try:
            accepted = self.pay(100007)
            require(accepted[0] == 202, f"Expected acceptance during Kafka outage, got HTTP {accepted[0]}")
            self.effects(json.loads(accepted[1]), 100007, 10000)
            deadline = time.monotonic() + 75
            while True:
                logs = self.c("logs", "--no-color", "--since", "90s", "payment-service")
                if "outbox publish failed, will retry" in logs:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Expected an actual outbox failure while Kafka is stopped")
                time.sleep(3)
            payment_id = json.loads(accepted[1])["id"]
            unpublished = self.query("db.getSiblingDB('payments').outbox.countDocuments({aggregate_id:"
                                     + json.dumps(payment_id) + ",published_at:null})")
            require(unpublished >= 1, "Outage payment had no unpublished outbox record")
        finally:
            self.c("start", "kafka")
        payment = self.terminal(accepted)
        require(payment["status"] == "COMPLETED", "Payment did not complete after Kafka restart")
        self.check("Kafka interruption and failed outbox publish", self.effects(payment, 100007, 9900))
        for topic in ("payments.requested", "ledger.results"):
            self.c("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092",
                   "--topic", topic, "--property", "parse.key=true", "--property", "key.separator=|",
                   data="100008|delivery-poison-" + uuid.uuid4().hex + "\n")
        payment = self.terminal(self.pay(100008))
        require(payment["status"] == "COMPLETED", "Valid payment did not complete after poison messages")
        for topic in ("payments.requested-dlt", "ledger.results-dlt"):
            deadline = time.monotonic() + 30
            while True:
                offsets = self.c("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                                 "--bootstrap-server", "localhost:9092", "--topic", topic)
                if sum(int(line.rsplit(":", 1)[1]) for line in offsets.splitlines() if line.startswith(topic + ":")) >= 1:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("Poison message did not reach DLT")
                time.sleep(1)
        self.check("poison messages route to both DLTs and processing continues", self.effects(payment, 100008, 9900))

    def finish(self):
        if self.started:
            if self.paused:
                self.c("unpause", "mongo", check=False)
            self.c("logs", "--no-color", check=False)
            self.c("ps", "-a", check=False)
            print("Containers and data kept. Stop with:", " ".join(self.compose + ["stop"]), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extended", action="store_true", help="also test process kill, MongoDB/Kafka interruption and both DLTs")
    parser.add_argument("--output", help="new directory for command logs and results")
    args = parser.parse_args()
    demo = Demo(args.output)
    try:
        demo.start()
        demo.basic()
        if args.extended:
            demo.extended()
        print(f"PASS: {len(demo.results)} scenarios; evidence: {demo.output}", flush=True)
    finally:
        demo.finish()


if __name__ == "__main__":
    main()
