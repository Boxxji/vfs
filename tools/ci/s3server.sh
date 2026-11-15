#!/bin/bash
# Builds the Hanzo S3 server the real-server tests run against and leaves it as
# $1/s3. It is built from a pinned commit, not taken from a release: the published
# releases are an older line without the own-storage server whose conditional
# writes the Head stands on, and `go install pkg@commit` is refused because that
# module carries a replace directive. A pinned commit is immutable, so the build is
# reproducible. The tests find the server as `s3` on PATH.
set -euo pipefail
commit=cbf3d650091a24f915aece9901e77bfef785721c
out=${1:?usage: s3server.sh DIRECTORY}
src=$(mktemp -d)
git clone --quiet --filter=blob:none https://github.com/hanzoai/s3 "$src"
git -C "$src" checkout --quiet "$commit"
mkdir -p "$out"
(cd "$src" && CGO_ENABLED=0 go build -trimpath -o "$out/s3" ./s3)
