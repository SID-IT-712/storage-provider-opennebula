# SID-IT-712 fork of OpenNebula/storage-provider-opennebula

This fork exists to run **two fixes** that are not yet upstream, and to propose
them upstream from the exact commits we run.

## Fix 1: reconciler driver filter

Upstream's stuck-attachment reconciler (`pkg/csi/driver/attachment_reconciler.go`)
lists every VolumeAttachment in the cluster and checks only `pv.Spec.CSI != nil`.
A PV belonging to **any other CSI driver** passes, its volume handle is never among
OpenNebula's observed attachments, and the stale-VolumeAttachment loop deletes it.

On a cluster running this driver beside Longhorn, it deleted 1,214 of Longhorn's
VolumeAttachments at about 16 per minute and detached live volumes from running pods.

The fix indexes only this driver's PVs, compared against the driver instance's own
name, so the stale-VA, orphan-detach and multi-attach paths all skip other drivers.
A regression test fails without it.

## Fix 2: disk identity by kernel serial (re-stage + wrong-disk guard)

**What was wrong.** After a kubelet restart (a Talos in-place config apply, or
`talosctl service kubelet restart`) kubelet re-calls NodeStage on volumes it
already staged. `evaluateLocalDiskPath` checks the mount source's serial through
`deviceMatchesSerial`, which could only succeed through `udevadm` / `lsblk`. The
node container has no udev db, so both report no serial, the check always failed,
and every re-stage was refused with `stale local disk stage detected ... mount
source serial does not match`, even on the correct disk (lamp-lab 2026-10-21).
Running pods kept working; the cost was repeated FailedMount noise.

Beside it sat a latent wrong-disk hazard. When by-id missed, `Resolve` fell back to
`/dev/<volumeName>` (the alias), and NodeStage then ran `FormatAndMount` with no
identity check. `volumeName` is OpenNebula's disk TARGET. On our VMs the TARGET is
one letter after the kernel name, because the context CD-ROM takes TARGET `sda`
but the kernel calls it `sr0`. So the alias is the NEIGHBOURING volume's disk.
By-id matching was also a substring match, so `onecsi-43` could bind `onecsi-439`.

**What changed.** Identity is decided in one place, and a serial request is
located only by proof:
- `identifyDevice(device, serial)` is the only evidence function and returns
  match, mismatch or unknown. A serial read from the kernel's own sysfs decides
  on its own when there is one: SCSI VPD page 0x80
  (`/sys/block/<dev>/device/vpd_pg80`), else virtio-blk `/sys/block/<dev>/serial`,
  else NVMe `/sys/block/<dev>/device/serial`. It needs no udev db. Otherwise udev
  `ID_SERIAL_SHORT` / `ID_SCSI_SERIAL` and lsblk `SERIAL` (stdout only) are
  compared exactly. udev `ID_SERIAL` is never used, because it is model-prefixed.
  A blank serial (Longhorn iSCSI disks report 36 spaces) is unknown. Unknown is
  never a match.
- `locateBySerial(serial)` is the only way a serial request finds its disk:
  1. The one whole disk under `/sys/block` whose kernel serial is exactly the
     serial. loop, ram, zram, sr, dm- devices and partitions are skipped. Two such
     disks are an error, never a choice. This works without by-id links.
  2. Otherwise an exact `/dev/disk/by-id` name (a whole trailing token, never a
     substring), accepted unless an identity source contradicts it. This covers
     disks with no readable serial.
  3. Never the TARGET alias.
- `Resolve` returns the proof with the device. The cache re-proves every hit.
  NodeStage, block publish and recovery all refuse a serial request's device that
  has no proof. A serial request with no resolver fails instead of using the alias.
- The re-stage check runs `identifyDevice` on the mount source. A mismatch makes
  it stale. A match makes it healthy. Unknown counts as healthy only if the mount
  source is the same block device that `locateBySerial` proves.
- The operator report locates through the same `locateBySerial`. The SCSI rescan
  writes under the same sysfs root.

**Files:** `pkg/csi/driver/node_device_resolver.go`, `nodeserver.go`,
`nodeserver_localdisk_recovery.go`, `operator_commands.go`. Tests are in
`node_kernel_serial_test.go` and `node_serial_locate_test.go`, plus three updated
tests in `node_device_resolver_test.go` / `nodeserver_test.go`. In
`nodeserver_localdisk_recovery.go`, the `/dev/` prefix test now follows
`defaultDiskPath`. The production value is unchanged; the point is that tests
reach the serial check.
**Chart:** no change.
**Upstreamable:** yes. The code is upstream's (v0.2.0) and the change is generic.

## Branches

| Branch | What it is | Rule |
|---|---|---|
| `master` | upstream, untouched | Never commit here. Sync from `upstream/master`. |
| `fix/reconciler-driver-filter` | **only** fix 1, one commit on upstream | Upstream-clean: no fork plumbing, no private references. This is what an upstream PR would come from. |
| `fix/restage-serial-check` | fix 2's PR branch (PR #1, base `sid/release`) | Fix 2 is two code-only `fix(node):` commits (code and tests, no fork files), so they can be cherry-picked onto fix 1 for an upstream PR. Merge PR #1 with a rebase or a merge commit, never a squash, so those commits stay separate from the `docs(sid):` commits on `sid/release`. |
| `sid/release` | both fixes **plus** this file and our workflows | Our deploy line. Images are built from here. Never merge it into a fix branch. |

## Images

`ghcr.io/sid-it-712/opennebula-csi:sid-v<upstream>-<n>` — for example
`sid-v0.2.0-1` is upstream `v0.2.0` plus fix 1, build 1; `sid-v0.2.0-2` adds fix 2. Built by
`.github/workflows/sid-image.yaml` when a `sid-v*` tag is pushed on `sid/release`.

- **Never push a `v*.*.*` tag.** Upstream's `release-csi.yaml` fires on those; it
  would try to push to `ghcr.io/opennebula`, publish charts to our `gh-pages` and
  cut a release.
- The workflow **refuses to publish** unless the filter is present and its
  regression test passes. Our deployment tooling lets only images from this
  repository run the reconciler, so every image published here must carry the fix.

## Rebasing onto a new upstream release

1. `git fetch upstream --tags`, then rebase `fix/reconciler-driver-filter` onto the new tag.
2. Rebase `sid/release` onto the rebased fix-1 branch, keeping **every** commit:
   fix 2's two code commits (`fix(node): prove disk identity by kernel serial ...`
   and `fix(node): decide disk identity in one place ...`), and the fork-only
   commits (this file, the workflows). Before tagging, check that
   `git log --oneline <new-tag>..sid/release` still lists both fix-2 commits. If one
   is dropped, the re-stage failure and the alias path come back silently.
3. For an upstream PR of fix 2, cherry-pick both commits, in order, onto the
   rebased fix-1 branch. They carry no fork files.
4. Run both regression suites, the reconciler filter and every fix-2 test:
   `go test ./pkg/csi/driver/ -count=1 -run '^(TestKernelDeviceSerial|TestSidLab|TestIdentifyDevice|TestResolve|TestNodeDeviceResolver|TestRequireSerialProof|TestStageVolumeRestage|TestStageVolumeNeverUsesAlias|TestStageVolumeRefusesSerialRequestWithoutResolver|TestStageVolumeByIDProof|TestStageVolumeKeepsLocalDeviceReport|TestBlockPublishRefuses|TestReportAgrees|TestRecoverySequence)'`.
   Then tag `sid-v<new-upstream>-1`.
5. If upstream has merged an equivalent fix, retire this fork: pin upstream's image
   in our deployment tooling, and trust its repository to run the reconciler only
   after re-running the regression test against it.

## Chart

We use upstream's own chart (`helm/opennebula-csi`) unchanged; neither fix touches it. Its default image,
`nudevco/opennebula-csi:v0.5.15`, is a third-party build. Always set
`image.repository` and `image.tag`. Set the reconciler flag with `--set-string`: the
chart reads it through `| default true`, and a boolean `false` renders as `"true"`.
