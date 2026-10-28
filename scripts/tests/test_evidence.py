import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
import uuid
from unittest.mock import patch

import evidence


class EvidenceTest(unittest.TestCase):
    def credential_line(self):
        return "Using generated security " + "password: " + str(uuid.uuid4())

    def test_generated_password_redacted_without_losing_diagnostics(self):
        original = "startup warning\n" + self.credential_line() + "\nretry failure details\n"
        sanitized = evidence.sanitize_text(original)
        self.assertIn("startup warning", sanitized)
        self.assertIn("retry failure details", sanitized)
        self.assertIn("[REDACTED-CREDENTIAL]", sanitized)
        self.assertEqual([], evidence.findings(sanitized))
        self.assertEqual(sanitized, evidence.sanitize_text(sanitized))

    def test_json_remains_valid_and_authorization_is_sanitized(self):
        original = json.dumps({"message": self.credential_line(), "Authorization": "Basic " + "YWNjb3VudDpwYXNz"})
        sanitized = evidence.sanitize_text(original)
        self.assertEqual([], evidence.findings(sanitized))
        self.assertIn("message", json.loads(sanitized))

    def test_configured_nondefault_password_is_redacted(self):
        value = str(uuid.uuid4())
        with patch.dict(os.environ, {"ALICE_PASSWORD": value}):
            self.assertTrue(evidence.findings("password=" + value))
            self.assertNotIn(value, evidence.sanitize_text("password=" + value))

    def test_nested_archive_sanitization_and_detection(self):
        def pack(name, data):
            out = io.BytesIO()
            with tarfile.open(fileobj=out, mode="w:gz") as archive:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            return out.getvalue()
        original = pack("nested.tar.gz", pack("trace.json", json.dumps({"message": self.credential_line()}).encode()))
        self.assertTrue(evidence.scan_bytes(original, "fixture.tar.gz"))
        updated = evidence.sanitize_archive(original)
        self.assertEqual([], evidence.scan_bytes(updated, "fixture.tar.gz"))

    def test_private_original_and_updated_archive_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "public"
            private = Path(directory) / "private"
            root.mkdir()
            original = (self.credential_line() + "\nuseful failure\n").encode()
            with tarfile.open(root / "command-outputs.tar.gz", "w:gz") as archive:
                member = tarfile.TarInfo("command-001.log")
                member.size = len(original)
                archive.addfile(member, io.BytesIO(original))
            raw_archive = (root / "command-outputs.tar.gz").read_bytes()
            self.assertEqual(["command-outputs.tar.gz"], evidence.sanitize_tree(root, private))
            self.assertEqual(raw_archive, (private / "command-outputs.tar.gz").read_bytes())
            self.assertEqual(0o700, private.stat().st_mode & 0o777)
            self.assertEqual(0o600, (private / "command-outputs.tar.gz").stat().st_mode & 0o777)
            self.assertEqual([], evidence.scan_tree(root))
            evidence.manifests(root)
            self.assertIn("command-001.log", (root / "outputs.sha256").read_text())
            self.assertIn("command-outputs.tar.gz", (root / "MANIFEST.sha256").read_text())

    def test_raw_evidence_cannot_be_preserved_inside_repository(self):
        with self.assertRaises(ValueError):
            evidence.sanitize_tree(Path("unused"), Path(evidence.__file__).parent / "private")

    def test_capture_redacts_before_writing_and_preserves_exit_code(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "captured.log"
            line = self.credential_line()
            command = [sys.executable, "-c", "import sys; print(sys.argv[1]); sys.exit(7)", line]
            result = subprocess.run([sys.executable, "-B", str(Path(evidence.__file__)), "capture", str(output),
                                     "--", *command], capture_output=True, text=True)
            self.assertEqual(7, result.returncode)
            self.assertEqual([], evidence.findings(output.read_text()))
            self.assertNotIn(line.split(": ", 1)[1], result.stdout + result.stderr + output.read_text())

    def test_capture_redacts_multiline_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "captured.log"
            value = "-----BEGIN " + "PRIVATE KEY-----\nsynthetic-key-material\n-----END PRIVATE KEY-----"
            command = [sys.executable, "-c", "import sys; print(sys.argv[1])", value]
            result = subprocess.run([sys.executable, "-B", str(Path(evidence.__file__)), "capture", str(output),
                                     "--", *command], capture_output=True, text=True)
            self.assertEqual(0, result.returncode)
            self.assertNotIn("synthetic-key-material", output.read_text())


if __name__ == "__main__":
    unittest.main()
