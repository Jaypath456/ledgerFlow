#!/usr/bin/env python3
"""Collect, sanitize and audit distributable diagnostic evidence without printing credentials."""
import argparse
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile


RULES = (
    ("generated-password", re.compile(r"(?i)(using generated (?:security|authentication) password:[ \t]*)([^\s\"'<>\[\]{}(),;]+)")),
    ("authorization", re.compile(r"(?i)(\b(?:Basic|Bearer)[ \t]+)([A-Za-z0-9+/_.~-]{8,}=*)")),
    ("provider-key", re.compile(r"()(\b(?:sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|(?:AKIA|ASIA)[A-Z0-9]{16}|xox[baprs]-[A-Za-z0-9-]{10,}))")),
    ("private-key", re.compile(r"()(-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----)")),
)


def configured_secrets():
    # Public local defaults are reproducible fixtures, not undisclosed credentials.
    return tuple(value for key, default in (("ALICE_PASSWORD", "alice-local"), ("BOB_PASSWORD", "bob-local"))
                 if (value := os.environ.get(key)) and value != default)


def sanitize_text(text):
    for _, pattern in RULES:
        text = pattern.sub(lambda match: match.group(1) + "[REDACTED-CREDENTIAL]", text)
    for value in configured_secrets():
        text = text.replace(value, "[REDACTED-CREDENTIAL]")
    return text


def findings(text):
    found = []
    for kind, pattern in RULES:
        for match in pattern.finditer(text):
            found.append({"kind": kind, "line": text.count("\n", 0, match.start()) + 1})
    for value in configured_secrets():
        if value in text:
            found.append({"kind": "configured-password", "line": text.count("\n", 0, text.index(value)) + 1})
    return found


def sanitized_bytes(data):
    # Every modified byte sequence must round-trip as UTF-8. Binary members are audited separately.
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        if findings(data.decode("utf-8", errors="replace")):
            raise ValueError("Credential pattern in non-UTF-8 evidence; sanitize explicitly before distribution")
        return data
    return sanitize_text(text).encode("utf-8")


def safe_members(archive):
    members = archive.getmembers()
    for member in members:
        if member.name.startswith("/") or ".." in Path(member.name).parts or not (member.isfile() or member.isdir()):
            raise ValueError("Evidence archive contains an unsafe member")
    return members


def sanitize_archive(data):
    buffer = io.BytesIO()
    changed = False
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as source:
        with tarfile.open(fileobj=buffer, mode="w:gz") as target:
            for member in safe_members(source):
                if member.isdir():
                    target.addfile(copy.copy(member))
                    continue
                original = source.extractfile(member).read()
                updated = sanitize_archive(original) if member.name.endswith((".tar.gz", ".tgz")) else sanitized_bytes(original)
                changed |= original != updated
                info = copy.copy(member)
                info.size = len(updated)
                target.addfile(info, io.BytesIO(updated))
    return buffer.getvalue() if changed else data


def scan_bytes(data, name):
    if name.endswith((".tar.gz", ".tgz")):
        results = []
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            for member in safe_members(archive):
                if member.isfile():
                    results.extend(scan_bytes(archive.extractfile(member).read(), name + "::" + member.name))
        return results
    return [{"file": name, **item} for item in findings(data.decode("utf-8", errors="replace"))]


def scan_tree(root):
    return [item for path in sorted(root.rglob("*")) if path.is_file()
            for item in scan_bytes(path.read_bytes(), str(path.relative_to(root)))]


def write_atomic(path, data):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temp:
        temporary = Path(temp.name)
        temp.write(data)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def sanitize_tree(root, private):
    repo = Path(__file__).resolve().parents[1]
    if private.resolve().is_relative_to(repo):
        raise ValueError("Raw evidence must be kept outside the repository")
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    private.chmod(0o700)
    changed = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        original = path.read_bytes()
        updated = sanitize_archive(original) if path.name.endswith((".tar.gz", ".tgz")) else sanitized_bytes(original)
        if original == updated:
            continue
        backup = private / path.relative_to(root)
        backup.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with backup.open("xb") as out:
            backup.chmod(0o600)
            out.write(original)
        write_atomic(path, updated)
        changed.append(str(path.relative_to(root)))
    return changed


def manifests(root):
    for path in root.rglob("command-outputs.tar.gz"):
        with tarfile.open(path) as archive:
            rows = [hashlib.sha256(archive.extractfile(member).read()).hexdigest() + "  " + member.name
                    for member in sorted(safe_members(archive), key=lambda m: m.name) if member.isfile()]
        (path.parent / "outputs.sha256").write_text("\n".join(rows) + "\n")
    manifest = root / "MANIFEST.sha256"
    manifest.write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + str(path.relative_to(root)) + "\n"
                                for path in sorted(root.rglob("*")) if path.is_file() and path != manifest))


def capture(command, output):
    output.parent.mkdir(parents=True, exist_ok=True)
    # Buffer in memory so multi-line credentials can also be redacted before any disk write.
    with output.open("w") as log:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                              errors="replace") as process:
            raw_output, _ = process.communicate()
            result = process.returncode
            log.write(sanitize_text(raw_output))
    print(f"Evidence capture exited {result}: {output}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for name in ("scan", "manifest", "sanitize"):
        command = commands.add_parser(name)
        command.add_argument("root", type=Path)
        if name == "sanitize":
            command.add_argument("--private", type=Path, required=True)
    collector = commands.add_parser("capture")
    collector.add_argument("output", type=Path)
    collector.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.action == "capture":
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            parser.error("capture requires a command")
        return capture(command, args.output)
    if args.action == "sanitize":
        changed = sanitize_tree(args.root, args.private)
        manifests(args.root)
        print(json.dumps({"sanitized_files": changed, "private_originals": str(args.private)}))
    elif args.action == "manifest":
        manifests(args.root)
    found = scan_tree(args.root)
    print(json.dumps({"credential_findings": found}))
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
