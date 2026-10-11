#!/usr/bin/env python3
"""Verified binary installer; no git, compiler, package manager or source checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import tarfile
import tempfile
import urllib.request

REPO = 'Josepavese/videosluice'
MAX_BYTES = 128 * 1024 * 1024

def digest(path):
    with open(path, 'rb') as f:
        h = hashlib.sha256()
        while block := f.read(1024 * 1024): h.update(block)
        return h.hexdigest()

def fetch(url, path):
    if not url.startswith('https://github.com/' + REPO + '/releases/download/'):
        raise ValueError('untrusted release URL')
    with urllib.request.urlopen(url, timeout=60) as response, open(path, 'wb') as out:
        total = 0
        while block := response.read(1024 * 1024):
            total += len(block)
            if total > MAX_BYTES:
                raise ValueError('release exceeds download limit')
            out.write(block)

def unpack(archive, destination, expected):
    if not re.fullmatch('[0-9a-f]{64}', expected) or digest(archive) != expected:
        raise ValueError('archive SHA-256 mismatch')
    with tarfile.open(archive, 'r:gz') as bundle:
        members = bundle.getmembers()
        names = set()
        if sum(m.size for m in members) > MAX_BYTES or len(members) > 10000:
            raise ValueError('release exceeds extraction limit')
        for member in members:
            name = Path(member.name)
            if name.is_absolute() or '..' in name.parts or member.name in names or not (member.isfile() or member.isdir()) or member.mode & 0o7000:
                raise ValueError('unsafe archive member')
            names.add(member.name)
        # All members are prevalidated regular files/directories; works on Python 3.10.
        bundle.extractall(destination)
    manifest = json.loads((destination / 'release.json').read_text())
    if manifest['schema'] != 'videosluice.release.v1':
        raise ValueError('invalid release manifest')
    actual = {p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file() and p.name != 'release.json'}
    if actual != set(manifest['files']):
        raise ValueError('release file set differs from manifest')
    for name, sha in manifest['files'].items():
        if digest(destination / name) != sha:
            raise ValueError('release member hash mismatch: ' + name)
    if manifest['os'] != 'linux' or manifest['arch'] != host_arch():
        raise ValueError('release platform differs from host')
    # Extraction may inherit a builder's permissive umask. Immutable runtime
    # payloads always use canonical executable/read-only permissions.
    for path in destination.rglob('*'):
        if path.is_file(): path.chmod(0o755 if path.stat().st_mode & 0o111 else 0o644)
        elif path.is_dir(): path.chmod(0o755)
    return manifest

def host_arch():
    machine = platform.machine().lower()
    return {'x86_64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(machine, machine)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--version', default='latest')
    p.add_argument('--variant', choices=['portable', 'rdkx5'], default='portable')
    p.add_argument('--home', type=Path, default=Path(os.environ.get('VIDEOSLUICE_HOME', str(Path(os.environ.get('XDG_DATA_HOME', str(Path.home() / '.local/share'))) / 'videosluice'))))
    p.add_argument('--archive', type=Path)
    p.add_argument('--sha256')
    p.add_argument('--destination', type=Path, help='managed immutable payload only; no launcher or mutable data')
    p.add_argument('--runtime-only', action='store_true', help='with destination, install only executables/native libraries')
    p.add_argument('--launcher-dir', type=Path, default=Path.home() / '.local/bin')
    a = p.parse_args()
    if platform.system() != 'Linux' or host_arch() not in ('amd64', 'arm64'):
        p.error('this release supports Linux amd64/arm64; other PAL providers are not implemented')
    if bool(a.archive) != bool(a.sha256):
        p.error('--archive requires --sha256')
    if a.version != 'latest' and not re.fullmatch(r'v\d+\.\d+\.\d+', a.version):
        p.error('version must be latest or vMAJOR.MINOR.PATCH')
    home = a.home.expanduser().absolute()
    if a.runtime_only and not a.destination: p.error('--runtime-only requires --destination')
    with tempfile.TemporaryDirectory(prefix='videosluice-install-') as scratch:
        scratch = Path(scratch)
        archive, expected = a.archive, a.sha256
        if archive is None:
            endpoint = 'latest' if a.version == 'latest' else 'tags/' + a.version
            request = urllib.request.Request('https://api.github.com/repos/' + REPO + '/releases/' + endpoint, headers={'User-Agent': 'VideoSluice-installer'})
            with urllib.request.urlopen(request, timeout=60) as response:
                release = json.load(response)
            tag = release['tag_name']
            if not re.fullmatch(r'v\d+\.\d+\.\d+', tag):
                raise ValueError('invalid release tag')
            name = f'videosluice-{tag}-linux-{host_arch()}-{a.variant}.tar.gz'
            base = f'https://github.com/{REPO}/releases/download/{tag}/'
            checksums = scratch / 'SHA256SUMS'
            fetch(base + 'SHA256SUMS', checksums)
            matches = [line.split()[0] for line in checksums.read_text().splitlines() if len(line.split()) == 2 and line.split()[1] == name]
            if len(matches) != 1:
                raise ValueError('release checksum absent or ambiguous')
            expected = matches[0]
            archive = scratch / name
            fetch(base + name, archive)
        payload = scratch / 'payload'; payload.mkdir()
        manifest = unpack(archive, payload, expected)
        if manifest['variant'] != a.variant:
            raise ValueError('release variant mismatch')
        if a.destination:
            destination = a.destination.absolute()
            if destination.exists():
                if destination.is_symlink() or any(destination.iterdir()):
                    raise ValueError('payload destination must be a real empty directory')
            else:
                destination.mkdir(parents=True)
            if a.runtime_only:
                shutil.copy2(payload / 'videosluice', destination / 'videosluice')
                shutil.copytree(payload / 'bin', destination / 'bin')
            else:
                shutil.copytree(payload, destination, dirs_exist_ok=True)
            print(json.dumps({'installed': str(destination), 'release': manifest['version'], 'sha256': expected}))
            return
        home.mkdir(parents=True, exist_ok=True)
        for name in ('bin', 'configs', 'data', 'logs', 'artifacts', 'backups', 'tmp', 'releases'):
            (home / name).mkdir(exist_ok=True)
        target = home / 'releases' / expected
        if target.exists():
            if target.is_symlink(): raise ValueError('release path is a symlink')
            # Reinstall verifies the retained complete payload before activation.
            if (target / 'release.json').read_bytes() != (payload / 'release.json').read_bytes(): raise ValueError('retained release manifest differs')
            for name, sha in manifest['files'].items():
                if (target / name).is_symlink() or digest(target / name) != sha: raise ValueError('retained release is modified')
                (target / name).chmod(0o755 if (target / name).stat().st_mode & 0o111 else 0o644)
        else:
            staging = Path(tempfile.mkdtemp(prefix='.stage-', dir=home / 'releases'))
            try:
                shutil.copytree(payload, staging, dirs_exist_ok=True)
                staging.rename(target)
            finally:
                if staging.exists(): shutil.rmtree(staging)
        pending = home / '.current-next'
        if pending.is_symlink(): pending.unlink()
        elif pending.exists(): raise ValueError('unsafe activation path')
        pending.symlink_to(target)
        os.replace(pending, home / 'current')
        launcher = home / 'bin/videosluice'
        if launcher.exists() and not launcher.is_symlink(): raise ValueError('launcher is not installer-owned symlink')
        if launcher.is_symlink(): launcher.unlink()
        launcher.symlink_to(home / 'current/videosluice')
        a.launcher_dir.mkdir(parents=True, exist_ok=True)
        entry = a.launcher_dir / 'videosluice'
        if entry.exists() and not entry.is_symlink(): raise ValueError('PATH entry is not installer-owned symlink')
        if entry.is_symlink(): entry.unlink()
        entry.symlink_to(launcher)
        marker = '# VideoSluice user PATH'
        profile = Path.home() / '.profile'
        if a.launcher_dir == Path.home() / '.local/bin' and marker not in (profile.read_text() if profile.exists() else ''):
            with profile.open('a') as f: f.write('\n' + marker + '\nexport PATH="$HOME/.local/bin:$PATH"\n')
        print(f'Installed VideoSluice {manifest["version"]} ({a.variant}) in {home}\nLauncher: {entry}\nVerify: {entry} --help\nInspect: {entry} --discover-capabilities')

if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, tarfile.TarError) as error:
        raise SystemExit('VideoSluice install failed: ' + str(error))
