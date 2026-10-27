# SID-IT-712 fork of OpenNebula/storage-provider-opennebula

This fork exists to run **five fixes** that are not yet upstream, and to propose
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
  1. The kernel: the whole disks under `/sys/block` whose kernel serial is
     exactly the serial. loop, ram, zram, sr and dm- devices are skipped, and so
     is a SCSI device whose `device/state` is not `running`. Partitions are not
     top-level in `/sys/block`. Exactly one match is the answer. If its `/dev`
     node is not there yet, the lookup waits; it **never** falls through to by-id.
     Two matches are ambiguous: the lookup retries until the timeout, then refuses
     (`ambiguous: sdb, sdd carry serial X`). It never picks one.
  2. Only when no disk reports the serial, an exact `/dev/disk/by-id` name:
     the serial itself, `<bus/model>_<serial>`, or `virtio-<serial>`. A bare
     `-<serial>` suffix is not accepted, because serials contain `-`. The name is
     accepted unless an identity source contradicts it. This covers disks with
     no readable serial.
  3. Never the TARGET alias.
- `Resolve` returns the proof with the device. Serial requests never use the
  cache, because a sysfs read is cheap and is the proof itself. NodeStage, block
  publish and recovery all refuse a serial request's device that has no proof.
  A serial request with no resolver fails instead of using the alias.
- Error codes: ambiguity, a missing resolver and a missing proof are
  `FailedPrecondition`. Only a genuine not-found is `DeadlineExceeded`, and its
  message names the TARGET it did not use.
- The re-stage check runs `identifyDevice` on the mount source. A mismatch makes
  it stale. A match makes it healthy. Unknown counts as healthy only if the mount
  source is the same block device that `locateBySerial` proves.
- The operator report locates through the same `locateBySerial`, with the real
  exec. It shows a locate error instead of a stale device path. `DeviceSerialMoved`
  compares the proven device with the mount source as block devices. The SCSI
  rescan writes under the same sysfs root.

**Files:** `pkg/csi/driver/node_device_resolver.go`, `nodeserver.go`,
`nodeserver_localdisk_recovery.go`, `operator_commands.go`. Tests are in
`node_kernel_serial_test.go` and `node_serial_locate_test.go`, plus three updated
tests in `node_device_resolver_test.go` / `nodeserver_test.go`. In
`nodeserver_localdisk_recovery.go`, the `/dev/` prefix test now follows
`defaultDiskPath`. The production value is unchanged; the point is that tests
reach the serial check.
**Chart:** no change.
**Upstreamable:** yes. The code is upstream's (v0.2.0) and the change is generic.

## Fix 3: detached disk expansion is deferred to the next attach

**What was wrong.** `ControllerExpandVolume` on a persistent disk attached to no
VM failed `FailedPrecondition: ... not supported because one.image.update does
not change canonical image size`, even with `featureGates.detachedDiskExpansion`
on (the chart default). The external-resizer backed off, and the grow landed only
if a pod attached in time (SID-IT-712/k8s-provisioner#479).

**What changed.** With the gate on, `ExpandVolume`
(`pkg/csi/opennebula/persistentdisk.go`) records the requested size on the image
(`CSI_REQUESTED_SIZE_BYTES` / `_MB`, a template update only) and returns it. The
existing attach path, `restoreAttachedVolumeRequestedSize`, grows the disk to it
with `one.vm.diskresize` before `ControllerPublishVolume` returns, and fails the
attach if it cannot. `VolumeExists` already reports the larger of canonical and
requested size, so a retried expand is a no-op. With the gate off the refusal is
unchanged. Tests: `persistentdisk_detached_expand_test.go` (an XML-RPC fake).
**Chart:** README line for the gate only.
**Upstreamable:** yes, one code-only `fix(controller):` commit.

## Fix 4: a clone is sized from the source's effective size

**What was wrong.** `CloneVolume` (`pkg/csi/opennebula/persistentdisk.go`)
returned the source image's `SIZE` as the clone's CSI capacity and ignored the
requested capacity. `SIZE` is OpenNebula's canonical size, which an online grow or
a deferred detached grow (fix 3) leaves at the old value. OpenNebula's
`one.image.clone` copies the source's template, so a grown source's
`CSI_REQUESTED_SIZE_*` reached the clone and its next attach did grow the disk,
but `CreateVolume` reported the old size. Kubernetes requests a clone at least as
large as its source PVC, so cloning a grown PVC, or asking for a larger clone,
returned less capacity than requested (the external-provisioner treats that as a
failed create). A larger clone also never recorded the larger size, so nothing
grew it.

**What changed.** The clone's size is the larger of the source's effective size
(`VolumeExists`, canonical or requested) and the requested capacity. The datastore
free-space check uses that size. If it is larger than the clone's canonical size,
it is written to the clone as `CSI_REQUESTED_SIZE_BYTES` / `_MB`, the same as
fix 3, and `restoreAttachedVolumeRequestedSize` grows the disk on the next attach.
`CreateVolume` reports that size. A clone at the source's size is unchanged and
writes no metadata. Snapshot restore on disk volumes is still `Unimplemented`, so
it has no sizing path to fix. Tests: `persistentdisk_clone_size_test.go` (a
multi-image XML-RPC fake whose clone copies the template and canonical SIZE, as
OpenNebula's `Image::clone_template` does).
**Chart:** no change.
**Upstreamable:** yes, one code-only `fix(controller):` commit.

## Fix 5: NodeStage grows an existing filesystem to its device

**What was wrong.** A clone requested larger than its source (a 3Gi clone of a
2Gi source) bound at 3Gi, and `ControllerPublishVolume`'s
`restoreAttachedVolumeRequestedSize` grew the disk to 3Gi (fix 4), but the pod saw
a ~2Gi filesystem (lamp-lab01, image `sid-v0.2.0-4`, ops case 80 Phase F). The
clone carries its source's filesystem. `NodeStageVolume` only ran
`FormatAndMount`, which leaves an existing filesystem as it is, and Kubernetes calls
`NodeExpandVolume` only when a PVC's size is raised after provisioning, never for a
fresh volume. So nothing grew it.

**What changed.** `growStagedFilesystem` (`pkg/csi/driver/nodeserver.go`) runs
after a mount volume is staged: mount-utils `ResizeFs.NeedResize`, then `Resize`
when the filesystem is smaller than its device. It is the same resizer, through
the same `nodeResizeFS` seam and the same exec, that `NodeExpandVolume` uses, so it
runs the node plugin image's own binaries, never the host's: `blockdev`, `blkid`,
`dumpe2fs` and `resize2fs` for ext3/ext4, `xfs_io` and `xfs_growfs` (on the mount
point) for xfs. All come from the `apk add` line in `Dockerfile` (`util-linux`,
`e2fsprogs-extra`, `xfsprogs-extra`). It runs:
- after a fresh `FormatAndMount`;
- on the already-staged branch, so kubelet's retry after a failed grow grows it
  instead of reporting success on a small filesystem;
- after local RWO stale-mount recovery's own `FormatAndMount`
  (`nodeserver_localdisk_recovery.go`).

A failed check or grow fails NodeStage with `Internal` (recovery records a
failure), and the mount is left for the retry. A filesystem that already fills its
device is left alone. Block volumes and read-only (`ro`) stages are never grown.
Tests: `nodeserver_stage_grow_test.go`. The fix-2 helper `localDiskNode` and
`TestStageVolume` stub the resizer, because their fake execs script no `blockdev`.
**Chart:** no change.
**Upstreamable:** yes, one code-only `fix(node):` commit.

## Branches

| Branch | What it is | Rule |
|---|---|---|
| `master` | upstream, untouched | Never commit here. Sync from `upstream/master`. |
| `fix/reconciler-driver-filter` | **only** fix 1, one commit on upstream | Upstream-clean: no fork plumbing, no private references. This is what an upstream PR would come from. |
| `fix/restage-serial-check` | fix 2's PR branch (PR #1, base `sid/release`) | Fix 2 is three code-only `fix(node):` commits (code and tests, no fork files), so they can be cherry-picked onto fix 1 for an upstream PR. Merge PR #1 with a rebase or a merge commit, never a squash, so those commits stay separate from the `docs(sid):` commits on `sid/release`. |
| `fix/detached-expansion-deferred` | fix 3's PR branch (base `sid/release`) | One code-only `fix(controller):` commit plus a `docs(sid):` commit for this file. Merge with a rebase or a merge commit, never a squash, so the code commit stays cherry-pickable. |
| `fix/clone-requested-size` | fix 4's PR branch (base `sid/release`) | One code-only `fix(controller):` commit plus a `docs(sid):` commit for this file. Merge with a rebase or a merge commit, never a squash. |
| `fix/stage-resize-fs` | fix 5's PR branch (base `sid/release`) | One code-only `fix(node):` commit plus a `docs(sid):` commit for this file. Merge with a rebase or a merge commit, never a squash. |
| `sid/release` | every fix **plus** this file and our workflows | Our deploy line. Images are built from here. Never merge it into a fix branch. |

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
   fix 2's three code commits (`fix(node): prove disk identity by kernel serial ...`,
   `fix(node): decide disk identity in one place ...` and
   `fix(node): never fall back from a kernel serial match ...`), and the fork-only
   commits (this file, the workflows), and fix 3's
   `fix(controller): defer detached disk expansion to the next attach`, and fix 4's
   `fix(controller): size clones from the effective source size`, and fix 5's
   `fix(node): grow an existing filesystem to its device on stage`. Before tagging, check that
   `git log --oneline <new-tag>..sid/release` still lists all three fix-2 commits. If one
   is dropped, the re-stage failure and the alias path come back silently.
3. For an upstream PR of fix 2, cherry-pick all three commits, in order, onto the
   rebased fix-1 branch. They carry no fork files.
4. Run both regression suites, the reconciler filter and every fix-2 test:
   `go test ./pkg/csi/driver/ -count=1 -run '^(TestKernelDeviceSerial|TestSidLab|TestIdentifyDevice|TestResolve|TestLocate|TestByIDNameHasSerial|TestNodeDeviceResolver|TestRequireSerialProof|TestStageVolumeRestage|TestStageVolumeNeverUsesAlias|TestStageVolumeRefusesSerialRequestWithoutResolver|TestStageVolumeByIDProof|TestStageVolumeAmbiguous|TestStageVolumeKeepsLocalDeviceReport|TestBlockPublishRefuses|TestReportAgrees|TestClassifyDeviceSerialMoved|TestRecoverySequence)'`.
   Then fix 3's: `go test ./pkg/csi/opennebula/ -count=1 -run '^(TestExpandDetachedVolume|TestAttachAfterDeferredExpand)'`.
   Then fix 4's: `go test ./pkg/csi/opennebula/ -count=1 -run '^TestClone'`.
   Then fix 5's: `go test ./pkg/csi/driver/ -count=1 -run '^(TestStageVolumeGrows|TestStageVolumeLeaves|TestStageVolumeFailsWhenGrow|TestStageVolumeRestageGrows|TestStageVolumeBlockNeverGrows|TestStageVolumeReadOnlyNeverGrows|TestLocalDiskRecovery)'`.
   Then tag `sid-v<new-upstream>-1`.
5. If upstream has merged an equivalent fix, retire this fork: pin upstream's image
   in our deployment tooling, and trust its repository to run the reconciler only
   after re-running the regression test against it.

## Chart

We use upstream's own chart (`helm/opennebula-csi`) unchanged; no fix changes its templates. Its default image,
`nudevco/opennebula-csi:v0.5.15`, is a third-party build. Always set
`image.repository` and `image.tag`. Set the reconciler flag with `--set-string`: the
chart reads it through `| default true`, and a boolean `false` renders as `"true"`.
