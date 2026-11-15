#!/bin/bash
# Runs the mount tests on a kernel mount, as an unprivileged user.
#
#	mount.sh run|skip REGEXP [TEST ...]
#
# run or skip the tests REGEXP names; every TEST listed must pass. A skipped test
# fails the step (the one test that only serves as the mount's child excepted): a
# step that cannot mount is a step that failed, not one that passed.
#
# A CI runner is a sandbox whose kernel ignores the setuid bit and gives its user no
# capability, so the usual helper, fusermount3, cannot be installed or used. The
# runner does let its user make a user namespace, in which it holds every
# capability and may mount a file system, and /dev/fuse is open to everyone. So the
# tests run inside one (`unshare -Urm`), with tools/fusemount, which does what
# fusermount3 does in a namespace, on PATH under that name.
set -euo pipefail
mode=${1:?usage: mount.sh run|skip REGEXP [TEST ...]}
pattern=${2:?usage: mount.sh run|skip REGEXP [TEST ...]}
shift 2
command -v unshare >/dev/null || { echo "unshare is missing: the mount tests need a user namespace"; exit 1; }
work=$(mktemp -d)
mkdir -p "$work/bin"
go build -o "$work/bin/fusermount3" ./tools/fusemount
go test -tags fuse -c -o "$work/mount.test" ./pkg/mount
log="$work/mount.log"
(cd pkg/mount && unshare -Urm env PATH="$work/bin:$PATH" TMPDIR="$work" \
	"$work/mount.test" -test.count=1 -test.v -test.timeout=4m "-test.$mode=$pattern") 2>&1 | tee "$log"
if grep -E -- '^\s*--- SKIP' "$log" | grep -v TestMountChild; then
	echo "a mount test skipped: it did not run"
	exit 1
fi
for t in "$@"; do
	grep -q -- "--- PASS: $t " "$log" || { echo "did not pass: $t"; exit 1; }
done
