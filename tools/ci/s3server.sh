#!/bin/bash
# Leaves the Hanzo S3 server the real-server tests run against as $1/s3. The tests
# find it as `s3` on PATH.
#
# It is taken from the published image of release v1.0.41, pinned by digest, and not
# built: from a cold Go cache the build costs five minutes of a job, and the image
# costs seconds. crane reads the image's layers, so nothing needs docker. The release
# is the current line of the server (the GitHub releases named v1.0.x are an older
# one, and `go install pkg@commit` is refused because the module carries a replace
# directive), and every real-server test passes against it, as it does against a
# build of the main commit cbf3d650 that it replaced here.
set -euo pipefail
image=ghcr.io/hanzoai/s3@sha256:64527e846e568dfe2ee753d3521152384f9943c6d32eaec1d9ddd1ac15b88925
out=${1:?usage: s3server.sh DIRECTORY}
mkdir -p "$out/tools" "$out/root"
GOBIN="$out/tools" go install github.com/google/go-containerregistry/cmd/crane@v0.20.2
"$out/tools/crane" export --platform "linux/$(go env GOARCH)" "$image" - | tar -x -C "$out/root" usr/bin/s3
mv "$out/root/usr/bin/s3" "$out/s3"
rm -rf "$out/root"
"$out/s3" version
echo "s3 server ready in ${SECONDS}s"
