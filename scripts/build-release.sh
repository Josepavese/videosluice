#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
output="${1:?absolute output directory required}"
variant="${2:-portable}"
[[ "$output" = /* ]] || { echo 'output must be absolute' >&2; exit 2; }
case "$variant" in portable|rdkx5) ;; *) exit 2;; esac
[ "$(uname -s)" = Linux ] || { echo 'Linux PAL required' >&2; exit 2; }
arch="$(uname -m)"; case "$arch" in x86_64) arch=amd64;; aarch64) arch=arm64;; *) exit 2;; esac
[ "$variant" != rdkx5 ] || [ "$arch" = arm64 ] || exit 2
mkdir -p "$output"
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
stage="$scratch/payload"; mkdir -p "$stage/bin"
export GOWORK=off
cd "$root"
go build -trimpath -o "$stage/videosluice" ./cmd/video-broker
go build -trimpath -o "$stage/bin/video-ingest-gst" ./cmd/video-ingest-gst
VIDEO_BROKER_OUTPUT_DIR="$stage/bin" bash native/build_frame_peer.sh
VIDEO_BROKER_OUTPUT_DIR="$stage/bin" bash native/build_gst_tone_plugin.sh
if [ "$variant" = rdkx5 ]; then VIDEO_BROKER_OUTPUT_DIR="$stage/bin" bash native/build_rdkx5_hbn_bridge.sh; fi
# Public Go/native/Python SDK and regression sources come from the same commit.
# The asset is independently usable: no sibling checkout is needed.
python3 - "$root" "$stage" "$arch" "$variant" <<'PY'
import hashlib,json,os,pathlib,shutil,subprocess,sys
root,stage=map(pathlib.Path,sys.argv[1:3])
for name in subprocess.check_output(['git','ls-files','-z'],cwd=root).decode().split('\0'):
 if not name or name.startswith(('.github/','issues/','improvements/')): continue
 p=root/name
 if not p.is_file() or p.is_symlink(): continue
 out=stage/name; out.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,out)
files={p.relative_to(stage).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(stage.rglob('*')) if p.is_file()}
manifest={'schema':'videosluice.release.v1','version':root.joinpath('VERSION').read_text().strip(),'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'os':'linux','arch':sys.argv[3],'variant':sys.argv[4],'files':files}
(stage/'release.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
PY
python3 - "$stage" <<'PYLIC'
import json,pathlib,shutil,subprocess,sys
stage=pathlib.Path(sys.argv[1]); licenses=stage/'licenses';licenses.mkdir()
modules=json.loads(subprocess.check_output(['go','list','-m','-json','all']).decode().replace('}\n{','},\n{').join(['[',']']))
for module in modules:
 if module.get('Main'):continue
 directory=pathlib.Path(module['Dir'])
 for p in directory.glob('LICENSE*'):
  shutil.copy2(p,licenses/(module['Path'].replace('/','_')+'-'+p.name))
manifest=json.loads((stage/'release.json').read_text())
import hashlib
manifest['files'].update({p.relative_to(stage).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in licenses.iterdir()})
(stage/'release.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
PYLIC
version="v$(cat VERSION)"
archive="$output/videosluice-$version-linux-$arch-$variant.tar.gz"
tar -C "$stage" --sort=name --mtime='UTC 2026-01-01' --owner=0 --group=0 --numeric-owner -czf "$archive" .
# Strip './' from member names to keep one canonical manifest path spelling.
python3 - "$archive" <<'PY'
import os,sys,tarfile,tempfile
path=sys.argv[1]
with tempfile.NamedTemporaryFile(dir=os.path.dirname(path),delete=False) as f: tmp=f.name
try:
 with tarfile.open(path,'r:gz') as src,tarfile.open(tmp,'w:gz') as dst:
  for member in src:
   if member.name=='.':continue
   member.name=member.name.removeprefix('./')
   dst.addfile(member,src.extractfile(member) if member.isfile() else None)
 os.replace(tmp,path)
finally:
 if os.path.exists(tmp):os.unlink(tmp)
PY
(cd "$output" && sha256sum "$(basename "$archive")" > "$(basename "$archive").sha256")
printf '%s\n' "$archive"
