import ast
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

import demo
import evidence


class StopBeforeBurst(Exception):
    pass


class DemoSafetyTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.instance = object.__new__(demo.Demo)
        self.instance.project = "ledgerflow-delivery-fixture"
        self.instance.output = Path(self.directory.name)
        self.instance.started = False
        self.instance.results = []
        self.instance.c = Mock(return_value="")
        self.instance.command = Mock(return_value="")
        self.instance.query = Mock(return_value=0)

    def test_existing_containers_prevent_startup(self):
        self.instance.c.return_value = "existing-container"
        with self.assertRaises(demo.EnvironmentCollisionError):
            self.instance.start()
        self.instance.c.assert_called_once_with("ps", "-a", "-q")
        self.assertFalse(self.instance.started)

    def test_existing_volume_prevents_startup_even_without_containers(self):
        self.instance.command.return_value = "other-volume\nledgerflow-delivery-fixture_mongodata"
        with self.assertRaises(demo.EnvironmentCollisionError):
            self.instance.start()
        self.instance.c.assert_called_once_with("ps", "-a", "-q")
        self.assertFalse(self.instance.started)

    def test_full_history_violation_fails_before_pass_record(self):
        self.instance.mongo = Mock(return_value='{"invariant":"I4","detail":"negative balance"}')
        with self.assertRaises(demo.FinancialInvariantError):
            self.instance.check("corruption", {})
        self.assertEqual([], self.instance.results)
        self.assertFalse((self.instance.output / "results.json").exists())

    def test_posting_and_balance_mismatch_fails(self):
        self.instance.query.return_value = dict(postings=2, balance=-100)
        with self.assertRaises(demo.FinancialInvariantError):
            self.instance.effects(dict(id="fixture", status="COMPLETED"), 100001, 3500)

    def test_outbox_timeout_still_fails(self):
        self.instance.query.return_value = 1
        with patch.object(demo.time, "monotonic", side_effect=[0, 61]):
            with self.assertRaises(TimeoutError):
                self.instance.check("stuck", {})

    def prepare_auth_probes(self, unauthorized=401, unowned=403, conflict=422):
        first = (202, b'{"id":"fixture"}', {"Idempotent-Replayed": "false"})
        replay = (202, first[1], {"Idempotent-Replayed": "true"})
        self.instance.pay = Mock(side_effect=[first, first, first, replay, (conflict, b"", {}),
                                               (unauthorized, b"", {}), (unowned, b"", {})])
        self.instance.terminal = Mock(side_effect=[dict(status="COMPLETED"),
            dict(status="DECLINED", declineReason="AMOUNT_LIMIT"),
            dict(status="FAILED", declineReason="INSUFFICIENT_FUNDS")])
        self.instance.effects = Mock(return_value={})
        self.instance.check = Mock()

    def test_all_authorization_and_conflict_probes_execute(self):
        self.prepare_auth_probes()
        with patch.object(demo.threading, "Barrier", side_effect=StopBeforeBurst):
            with self.assertRaises(StopBeforeBurst):
                self.instance.basic()
        calls = self.instance.pay.call_args_list
        self.assertEqual(7, len(calls))
        self.assertEqual(1501, calls[4].args[1])
        self.assertEqual({"identity": None}, calls[5].kwargs)
        self.assertEqual({"identity": "bob"}, calls[6].kwargs)

    def test_wrong_authorization_and_conflict_responses_fail(self):
        for options in (dict(unauthorized=202), dict(unowned=202), dict(conflict=202)):
            with self.subTest(options=options):
                self.prepare_auth_probes(**options)
                with self.assertRaises(demo.DemoCheckError):
                    self.instance.basic()

    def test_demo_contains_no_optimization_sensitive_assertions(self):
        tree = ast.parse(Path(demo.__file__).read_text())
        self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(tree)))

    def test_command_output_is_sanitized_before_persistence(self):
        value = str(uuid.uuid4())
        raw = "Using generated security " + "password: " + value + "\nuseful failure details\n"
        self.instance.command_number = 0
        self.instance.env = {}
        result = subprocess.CompletedProcess(["fixture"], 0, stdout=raw)
        with patch.object(demo.subprocess, "run", return_value=result):
            demo.Demo.command(self.instance, ["fixture"])
        stored = (self.instance.output / "command-001.log").read_text()
        self.assertNotIn(value, stored)
        self.assertIn("useful failure details", stored)
        self.assertEqual([], evidence.findings(stored))


class ComposeCompatibilityTest(unittest.TestCase):
    def render(self, *files, source=None):
        command = ["docker", "compose", "--project-directory", str(demo.ROOT / "infra")]
        for file in files:
            command += ["-f", str(file)]
        environment = dict(os.environ, KAFKA_PORT="49992")
        return json.loads(subprocess.check_output(command + ["config", "--format", "json"],
                          input=source, text=True, env=environment))["services"]["kafka"]

    def test_default_broker_exactly_matches_original_configuration(self):
        original = subprocess.check_output(["git", "show", "HEAD:infra/docker-compose.yml"], cwd=demo.ROOT, text=True)
        self.assertEqual(self.render("-", source=original), self.render(demo.ROOT / "infra/docker-compose.yml"))

    def test_only_isolated_overlay_uses_loopback_and_alternate_port(self):
        broker = self.render(demo.ROOT / "infra/docker-compose.yml", demo.ROOT / "infra/docker-compose.delivery.yml")
        self.assertEqual(1, len(broker["ports"]))
        self.assertEqual("127.0.0.1", broker["ports"][0]["host_ip"])
        self.assertEqual("49992", broker["ports"][0]["published"])
        self.assertIn("EXTERNAL://localhost:49992", broker["environment"]["KAFKA_ADVERTISED_LISTENERS"])


if __name__ == "__main__":
    unittest.main()
