#!/usr/bin/env python3
import json
from pathlib import Path
import subprocess
import sys
import tempfile

archive, sha, variant = sys.argv[1:4]
installer = Path(__file__).resolve().parents[1] / 'install/install.py'
with tempfile.TemporaryDirectory(prefix='videosluice-smoke-') as work:
    root = Path(work)
    home = root / 'home'; launchers = root / 'path'
    cmd = [sys.executable, '-B', str(installer), '--archive', archive, '--sha256', sha, '--variant', variant, '--home', str(home), '--launcher-dir', str(launchers)]
    subprocess.run(cmd, check=True)
    subprocess.run([str(launchers / 'videosluice'), '--help'], check=True, stdout=subprocess.DEVNULL)
    for path in ('data/operator-state', 'configs/operator.json'):
        (home / path).write_text('preserve this')
    subprocess.run(cmd, check=True)
    for path in ('data/operator-state', 'configs/operator.json'):
        assert (home / path).read_text() == 'preserve this'
    before = (home / 'current').resolve()
    failed = subprocess.run(cmd[:cmd.index('--sha256')+1] + ['0'*64] + cmd[cmd.index('--sha256')+2:], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert failed.returncode != 0 and (home / 'current').resolve() == before
    destination = root / 'payload'
    subprocess.run(cmd + ['--destination', str(destination)], check=True)
    assert (destination / 'videosluice').is_file() and (destination / 'bin/libvideo_broker_frame_peer.so').is_file()
    manifest = json.loads((destination / 'release.json').read_text())
    subprocess.run([str(destination / 'videosluice'), '--check-config', '--config', str(destination / 'examples/uvc-mjpeg.json')], check=True, stdout=subprocess.DEVNULL)
    print('PASS fresh/reinstall/preserved-data/preserved-config/bad-hash/payload/config:', manifest['version'], manifest['arch'], manifest['variant'])
