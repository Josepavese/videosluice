#!/usr/bin/env python3
"""Reject unreviewed module dependencies and broken local Markdown references."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[1]
module = "github.com/Josepavese/videosluice"
approved = (module, "github.com/pion/rtp", "github.com/pion/randutil", "golang.org/x/sys")
if (root / ".git").exists():
    paths = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root, text=True).splitlines()
else:
    paths = [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file() and ".git" not in p.parts]
failures = []
for name in paths:
    path = root / name
    if not path.is_file():
        continue
    if path.suffix == ".go":
        text = path.read_text()
        blocks = re.findall(r'(?ms)^import\s*\((.*?)^\)', text)
        blocks += re.findall(r'(?m)^import\s+(.*)$', text)
        for block in blocks:
            for dependency in re.findall(r'"([^"\n]+)"', block):
                if "." in dependency.split("/")[0] and not any(dependency == p or dependency.startswith(p + "/") for p in approved):
                    failures.append(f"{name}: unreviewed dependency {dependency}")
    if path.suffix == ".md":
        for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)', path.read_text()):
            target = target.split("#", 1)[0].split(" ", 1)[0].strip("<>")
            if target and ":" not in target and not target.startswith("/") and not (path.parent / target).exists():
                failures.append(f"{name}: missing local reference {target}")
if failures:
    raise SystemExit("\n".join(failures))
print("PASS reviewed dependencies and local documentation references")
