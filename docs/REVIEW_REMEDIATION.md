# Independent review remediation — 2026-10-28

Scope: exactly the three reviewed findings. PaymentSecurity, account ownership, MongoDB velocity admission, transaction retries, ledger posting, Kafka keys and financial invariant logic were not changed in this remediation. Earlier uncommitted work was preserved. Nothing was committed or pushed.

## 1. Existing Kafka data preserved without migration

Before editing files, inspected the existing stopped `ledgerflow-kafka-1`: container identity/configuration, listener values, mounts/volumes, topic directories, metadata, recovery/replication checkpoints and all data-file contents. Topic partitions and committed consumer offsets were also decoded offline from read-only copies; the existing broker was never started. A disposable tool container ran only `kafka-dump-log.sh`, with no network, a read-only root filesystem and a read-only bind mount. It was not a broker.

The existing broker uses `/tmp/kafka-logs`; its anonymous `/var/lib/kafka/data` mount does not cover those files. Changing the existing broker's bind address would require recreation. Instead, the base Kafka configuration now exactly matches its original committed configuration: `29092:29092` and `EXTERNAL://localhost:29092`. Its rendered configuration hash is:

```text
7f9654c560e92c60704292d31bae6caf30fbf3f0a4f1f8d1d5a76d302ffc86e2
```

This matches the existing container's Compose label. The new delivery overlay changes Kafka binding only for fresh, collision-protected demo projects. Regression tests compare the default Kafka service with `git show HEAD:infra/docker-compose.yml`, including when `KAFKA_PORT` is set, and verify the overlay replaces the port list with one loopback binding.

No migration, backup/restore operation against the broker, or migration approval was needed. No migration tool was introduced. A later request to persist or harden the original broker would require a separate reversible procedure, disposable migration testing, and approval before touching that broker.

Preservation evidence: [before](evidence/remediation/broker-before.json), [after](evidence/remediation/broker-after.json), [topics/offsets and comparison](evidence/remediation/broker-preservation.json), [offline committed-offset decoding](evidence/remediation/broker-offsets-offline.log).

- Container `b3bfb59b39fadac06babfafd49ac55e206eb82f77899fe3f007c6c4b77779f58` retains its identity, creation/start/finish timestamps, stopped state, restart count and mount identities.
- **331 data files / 106562 bytes** retain identical content hashes. Sorted inventory SHA-256: `28701ead74acfe8664e5afb318def2e0489ba05c15b029190d37624c357aa4b1`.
- Request topic recovery offsets by partition 0/1/2: **0 / 1 / 28**; result offsets: **11 / 8 / 10**. Both DLTs retain three partitions with offsets zero. Consumer commits are recorded in the comparison JSON; the offset log contains no commit for request partition 0.
- No `up`, `start`, `restart`, `stop`, `kill`, `down`, `rm`, volume removal or restore operation targeted the original broker. Inspection used `inspect`, `cp` to memory/private copies, and local `compose config` rendering only.

The first offline decoder attempt rejected renamed segment basenames with `NumberFormatException`. The rerun preserved Kafka's numeric segment filenames and succeeded. The [initial diagnostic](evidence/remediation/broker-offsets-offline-initial.log) is retained. Neither attempt wrote to the original broker or source copies.

Inspection commands included:

```sh
docker inspect ledgerflow-kafka-1
docker cp -L ledgerflow-kafka-1:/opt/kafka/config/server.properties -
docker cp ledgerflow-kafka-1:/tmp/kafka-logs -
docker volume ls --format '{{.Name}}'
docker compose -f infra/docker-compose.yml config --hash kafka
# On private copies preserving __consumer_offsets-*/00000000000000000000.log:
docker run --rm --pull never --network none --hostname localhost --user 0:0 \
  --read-only --tmpfs /tmp --entrypoint /opt/kafka/bin/kafka-dump-log.sh \
  -v "$inspection_dir:/inspection:ro" apache/kafka:4.0.0 \
  --files /inspection/__consumer_offsets-32/00000000000000000000.log,/inspection/__consumer_offsets-47/00000000000000000000.log \
  --deep-iteration --print-data-log --offsets-decoder
```

`docker cp ... -` was read as an in-memory tar stream. Read-only inspection copies lived under private `/private/tmp/ledgerflow-broker-inspection-*` directories, not in the repository.

## 2. Smoke checks remain effective with optimization

Every assertion in `scripts/demo.py` was replaced with explicit checks. Environment collisions raise `EnvironmentCollisionError`; balance/posting/invariant failures raise `FinancialInvariantError`; other unexpected results raise `DemoCheckError`; time limits raise `TimeoutError`. HTTP requests, including unauthorized and conflicting requests, execute before their response checks.

Tests prove that existing containers and volumes prevent any startup, nonempty invariant output prevents a PASS record, wrong balances/postings fail, outbox timeouts fail, and authorization/conflict probes actually execute and reject unexpected statuses. Tests also verify sanitization before command output is saved, archive redaction/private preservation/manifests, and the unchanged base Kafka configuration.

Executed commands:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=scripts python3 -B -m unittest discover -s scripts/tests -v
PYTHONDONTWRITEBYTECODE=1 PYTHONOPTIMIZE=1 PYTHONPATH=scripts python3 -B -m unittest discover -s scripts/tests -v
```

**19 tests passed in each mode**, zero failures/errors. Logs: [normal](evidence/remediation/python-normal.log), [optimized](evidence/remediation/python-optimized.log). These tests use mocks/disposable files for negative safety cases; they never attempt a collision against a real existing broker.

## 3. Credentials removed from distributable evidence

Scanned all tracked/proposed evidence, including text logs, JSON and compressed tar members. Exactly three existing files contained generated authentication passwords:

- `docs/evidence/delivery/final-verify.log`
- `docs/evidence/delivery/ordering-recheck.log`
- `docs/evidence/delivery/ordering-verify.log`

Credential values are intentionally omitted. Their distributable copies now retain the diagnostic message with a redaction marker. Original copies are retained privately under `/private/tmp/ledgerflow-private-evidence-mn13a77g`, outside the repository: directory mode 0700, files 0600. This temporary private location is not a long-term retention guarantee. It is never included in manifests or archives intended for Git.

The original delivery archive contained no generated credentials and was preserved byte-for-byte; its member hashes were regenerated. Nested archive handling is covered by synthetic regression fixtures. Public documented local demo/test credential constants remain part of reproducible examples; generated passwords, authentication headers, recognized provider/private keys and configured nondefault demo passwords are redacted by the collector/audit rules.

`Demo.command` sanitizes outputs before writing logs or error messages. `scripts/evidence.py capture` buffers child output in memory, redacts it before disk persistence, and preserves the child exit code. Raw Maven reports under ignored `target/` directories are not distributable evidence; use the collector and scan before copying diagnostics into Git artifacts.

Commands used:

```sh
python3 -B scripts/evidence.py scan docs/evidence
python3 -B scripts/evidence.py sanitize docs/evidence \
  --private /private/tmp/ledgerflow-private-evidence-mn13a77g
python3 -B scripts/evidence.py manifest docs/evidence
python3 -B scripts/evidence.py scan docs/evidence
```

Evidence: [initial location-only scan](evidence/remediation/credential-scan-before.json), [sanitization result](evidence/remediation/sanitization.json), [final audit](evidence/remediation/final-audit.json), [all-artifact manifest](evidence/MANIFEST.sha256). Both delivery archives have regenerated `outputs.sha256` member manifests.

## Final automated and live verification

```sh
JAVA_HOME=$(/usr/libexec/java_home -v 25) PYTHONDONTWRITEBYTECODE=1 \
  python3 -B scripts/evidence.py capture docs/evidence/remediation/maven-verify.log -- ./mvnw -B verify
PAYMENT_PORT=18281 LEDGER_PORT=18282 MONGO_PORT=37217 KAFKA_PORT=39292 \
  PYTHONDONTWRITEBYTECODE=1 PYTHONOPTIMIZE=1 \
  python3 -B scripts/evidence.py capture docs/evidence/remediation/demo-optimized.log -- \
  bash scripts/smoke.sh --extended --output docs/evidence/remediation/system
git diff --check
```

- Maven: **66 tests, 0 failures, 0 errors, 0 skipped**, exit 0; 1 minute 51 seconds, completed 2026-10-28 16:33:38 America/New_York. [Sanitized log](evidence/remediation/maven-verify.log).
- Live optimized-Python demo: **11/11 scenarios passed**, with I1–I6 checked after each. Includes real service interruptions, MongoDB pause, Kafka publish failure/recovery and both poison-message DLTs. Only the new `ledgerflow-delivery-30168829bc` project was faulted. [Log](evidence/remediation/demo-optimized.log), [results](evidence/remediation/system/results.json), [Docker commands](evidence/remediation/system/commands.jsonl), [archived command outputs](evidence/remediation/system/command-outputs.tar.gz).
- Final isolated history: **40 payments: 11 COMPLETED, 1 FAILED, 28 DECLINED**. Pending and unpublished: **0**. Ledger transactions/entries including funding: **22 / 44**. Global entry sum, duplicate posting groups and negative non-SYSTEM balances: **0**. Full I1–I6 checker: **0 violations**. [State](evidence/remediation/financial-state.json), [checker output](evidence/remediation/invariants.log).
- Original broker comparison: unchanged, including all data hashes and default configuration hash. No destructive Compose operation targeted an existing broker.
- Final evidence audit and SHA-256 verification are recorded in `final-audit.json`; `git diff --check` passed.

The new demo remains healthy at `http://127.0.0.1:18281`. To stop only this run without removing data:

```sh
docker compose -p ledgerflow-delivery-30168829bc \
  -f infra/docker-compose.yml -f infra/docker-compose.delivery.yml stop
```

## Affected files and remaining limits

Remediation changes only `infra/docker-compose.yml`, new `infra/docker-compose.delivery.yml`, `scripts/demo.py`, new `scripts/evidence.py`, two new `scripts/tests/` modules, README/decision/result/delivery documentation, this report, and evidence/manifests. No Java service, MongoDB transaction, payment contract or financial invariant implementation changed in this remediation.

The original Kafka binding intentionally retains its previous exposure and its log data remains in the container writable layer. The isolated demo overlay binds Kafka to loopback. Recreating or securing the old broker itself remains a separate, approval-gated migration. Evidence scanning covers known credential formats and configured demo secrets; it is not a general proof against arbitrary encoded secrets. Original raw evidence is private temporary data. Existing volumes and prior demonstration projects remain preserved.
