package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// withTestSysfs points nodeSysfsRoot at an empty temp tree for the test.
func withTestSysfs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	original := nodeSysfsRoot
	nodeSysfsRoot = root
	t.Cleanup(func() { nodeSysfsRoot = original })
	require.NoError(t, os.MkdirAll(filepath.Join(root, "block"), 0o755))
	return root
}

// vpdPage80 builds an SPC unit-serial-number VPD page (0x80).
func vpdPage80(serial string) []byte {
	return append([]byte{0x00, 0x80, byte(len(serial) >> 8), byte(len(serial))}, []byte(serial)...)
}

// writeSysfsVPD80 writes raw bytes as <root>/block/<name>/device/vpd_pg80.
func writeSysfsVPD80(t *testing.T, root, name string, raw []byte) {
	t.Helper()
	dir := filepath.Join(root, "block", name, "device")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "vpd_pg80"), raw, 0o444))
}

// writeSysfsSerial writes <root>/block/<name>/serial (virtio-blk).
func writeSysfsSerial(t *testing.T, root, name, serial string) {
	t.Helper()
	dir := filepath.Join(root, "block", name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "serial"), []byte(serial), 0o444))
}

// writeDisk creates a fake device node under the test disk path.
func writeDisk(t *testing.T, diskPath, name string) string {
	t.Helper()
	p := filepath.Join(diskPath, name)
	require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	resolved, err := filepath.EvalSymlinks(p)
	require.NoError(t, err)
	return resolved
}

// linkByID creates <diskPath>/disk/by-id/<name> -> <diskPath>/<target>.
func linkByID(t *testing.T, diskPath, name, target string) {
	t.Helper()
	dir := filepath.Join(diskPath, "disk", "by-id")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.Symlink(filepath.Join(diskPath, target), filepath.Join(dir, name)))
}

// steppingResolverClock makes every deviceResolverNow call advance one
// second, so a resolver timeout expires after a few loop iterations instead
// of spinning in real time.
func steppingResolverClock(t *testing.T) {
	t.Helper()
	original := deviceResolverNow
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	deviceResolverNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(time.Second)
		return now
	}
	t.Cleanup(func() { deviceResolverNow = original })
}

// recordingExec is a FakeExec with empty output for every command (no udev db,
// lsblk knows no serial: what the node container sees) that records the
// commands it was asked to run.
type recordingExec struct {
	testingexec.FakeExec
	mu   sync.Mutex
	cmds []string
}

func newRecordingExec() *recordingExec {
	return &recordingExec{FakeExec: testingexec.FakeExec{DisableScripts: true, LookPathFunc: func(p string) (string, error) { return p, nil }}}
}

func (r *recordingExec) Command(cmd string, args ...string) exec.Cmd {
	r.mu.Lock()
	r.cmds = append(r.cmds, cmd)
	r.mu.Unlock()
	return r.FakeExec.Command(cmd, args...)
}

func (r *recordingExec) CommandContext(_ context.Context, cmd string, args ...string) exec.Cmd {
	return r.Command(cmd, args...)
}

// formatOrMountCommands lists the commands FormatAndMount would run (disk
// probing and mkfs). None may appear when staging is refused.
func (r *recordingExec) formatOrMountCommands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for _, c := range r.cmds {
		if c == "blkid" || strings.HasPrefix(c, "mkfs") || c == "fsck" || c == "mount" {
			out = append(out, c)
		}
	}
	return out
}

func TestKernelDeviceSerial(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, root string)
		want  string
	}{
		{"vpd_pg80 valid", func(t *testing.T, root string) { writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-20")) }, "onecsi-20"},
		{"vpd_pg80 short buffer", func(t *testing.T, root string) { writeSysfsVPD80(t, root, "sdb", []byte{0x00, 0x80, 0x00}) }, ""},
		{"vpd_pg80 wrong page code", func(t *testing.T, root string) {
			raw := vpdPage80("onecsi-20")
			raw[1] = 0x83
			writeSysfsVPD80(t, root, "sdb", raw)
		}, ""},
		{"vpd_pg80 length larger than buffer", func(t *testing.T, root string) {
			raw := vpdPage80("onecsi-20")
			raw[3] = 0x40
			writeSysfsVPD80(t, root, "sdb", raw)
		}, ""},
		{"vpd_pg80 trailing spaces and NULs", func(t *testing.T, root string) {
			writeSysfsVPD80(t, root, "sdb", vpdPage80(" onecsi-20  \x00\x00"))
		}, "onecsi-20"},
		{"vpd_pg80 ignores bytes past the declared length", func(t *testing.T, root string) {
			writeSysfsVPD80(t, root, "sdb", append(vpdPage80("onecsi-20"), []byte("9")...))
		}, "onecsi-20"},
		{"virtio serial", func(t *testing.T, root string) { writeSysfsSerial(t, root, "sdb", "onecsi-7\n") }, "onecsi-7"},
		{"no serial source", func(t *testing.T, root string) {
			require.NoError(t, os.MkdirAll(filepath.Join(root, "block", "sdb"), 0o755))
		}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := withTestSysfs(t)
			diskPath := withTestDiskPath(t)
			dev := writeDisk(t, diskPath, "sdb")
			c.setup(t, root)
			got, err := kernelDeviceSerial(dev)
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestKernelDeviceSerialPartitionIsUnknown(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-20"))
	part := writeDisk(t, diskPath, "sdb1")
	got, err := kernelDeviceSerial(part)
	require.NoError(t, err)
	assert.Equal(t, "", got)
}

func TestKernelDeviceSerialFollowsSymlink(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	writeDisk(t, diskPath, "sdb")
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-20"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-20", "sdb")
	got, err := kernelDeviceSerial(filepath.Join(diskPath, "disk", "by-id", "scsi-0QEMU_QEMU_HARDDISK_onecsi-20"))
	require.NoError(t, err)
	assert.Equal(t, "onecsi-20", got)
}

// localDiskNode builds a node server for local-disk stage tests: a recording
// exec with no udev db, a fake mounter (optionally with the staging path
// already mounted from mountSource), and a recorded session for vol-1.
func localDiskNode(t *testing.T, diskPath, mountSource string) (*NodeServer, *mount.FakeMounter, *recordingExec, string) {
	t.Helper()
	staging := t.TempDir()
	mounts := []mount.MountPoint{}
	if mountSource != "" {
		mounts = append(mounts, mount.MountPoint{Path: staging, Device: filepath.Join(diskPath, mountSource), Type: "ext4"})
	}
	fm := mount.NewFakeMounter(mounts)
	rec := newRecordingExec()
	ns := NewNodeServer(getTestNodeServer(nil).Driver, &mount.SafeFormatAndMount{Interface: fm, Exec: rec})
	ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
	if mountSource != "" {
		ns.recordLocalDiskSession(localDiskSession{VolumeID: "vol-1", StagingTargetPath: staging, DeviceSerial: "onecsi-11", FSType: "ext4"})
	}
	return ns, fm, rec, staging
}

// stageRequest uses OpenNebula's disk TARGET as volumeName. On the measured
// VMs the TARGET is one letter after the kernel name (TARGET sdc = kernel sdb).
func stageRequest(staging, target string) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: staging,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		PublishContext: map[string]string{
			"volumeName":                                target,
			annotationBackend:                           "local",
			publishContextDeviceSerial:                  "onecsi-11",
			publishContextDeviceDiscoveryTimeoutSeconds: "3",
		},
	}
}

// Live bug (lamp-lab 2026-10-21): after a kubelet restart NodeStage is called
// on the already-mounted staging path. With no udev db in the container the
// serial check could never pass and the correct disk was refused.
func TestStageVolumeRestageCorrectDiskWithoutUdev(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdb")
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-11"))
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-12"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-11", "sdb")
	ns, fm, rec, staging := localDiskNode(t, diskPath, "sdb")

	resp, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "sdc"))
	require.NoError(t, err)
	assert.Equal(t, &csi.NodeStageVolumeResponse{}, resp)
	assert.Empty(t, fm.GetLog())
	assert.Empty(t, rec.formatOrMountCommands())
}

// The mounted device's kernel serial is another volume's: the re-stage is
// refused by the serial check (not by the unexpected-source fallback).
func TestStageVolumeRestageRejectsMountedDeviceWithOtherKernelSerial(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdb")
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-12"))
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-11"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-11", "sdc")
	ns, fm, rec, staging := localDiskNode(t, diskPath, "sdb")

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "sdd"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "serial does not match")
	assert.Empty(t, fm.GetLog())
	assert.Empty(t, rec.formatOrMountCommands())
}

// Latent wrong-disk hazard: by-id is absent and /dev/<TARGET> exists but is the
// neighbouring volume's disk. A serial request must never fall back to the
// alias, so nothing is formatted or mounted.
func TestStageVolumeNeverUsesAliasWhenSerialRequested(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-99"))
	ns, fm, rec, staging := localDiskNode(t, diskPath, "")

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "sdc"))
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Empty(t, fm.GetLog(), "FormatAndMount must not have been called")
	assert.Empty(t, rec.formatOrMountCommands(), "FormatAndMount must not have been called")
}

// Whatever path resolution took (here the resolver-less alias fallback), a
// device whose kernel serial is not the requested one is never formatted.
func TestStageVolumeRefusesResolvedDeviceWithWrongKernelSerial(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-99"))
	ns, fm, rec, staging := localDiskNode(t, diskPath, "")
	ns.deviceResolver = nil

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "sdc"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "does not carry serial onecsi-11")
	assert.Empty(t, fm.GetLog(), "FormatAndMount must not have been called")
	assert.Empty(t, rec.formatOrMountCommands(), "FormatAndMount must not have been called")
}

// onecsi-43 must never bind onecsi-439's disk, whatever order the by-id glob
// returns, and whether or not the kernel serial is available to catch it.
func TestResolveByIDPrefixCollision(t *testing.T) {
	for _, withSysfs := range []bool{true, false} {
		for _, reversed := range []bool{false, true} {
			root := withTestSysfs(t)
			diskPath := withTestDiskPath(t)
			writeDisk(t, diskPath, "sdb")
			want := writeDisk(t, diskPath, "sdc")
			if withSysfs {
				writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-439"))
				writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-43"))
			}
			linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-439", "sdb")
			linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-43", "sdc")
			original := nodeByIDGlob
			nodeByIDGlob = func(pattern string) ([]string, error) {
				m, err := original(pattern)
				if reversed {
					for i, j := 0, len(m)-1; i < j; i, j = i+1, j-1 {
						m[i], m[j] = m[j], m[i]
					}
				}
				return m, err
			}
			r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), nil)
			got, ok := r.resolveByID("onecsi-43")
			nodeByIDGlob = original
			require.True(t, ok, "sysfs=%v reversed=%v", withSysfs, reversed)
			assert.Equal(t, want, got.DevicePath, "sysfs=%v reversed=%v", withSysfs, reversed)
		}
	}
}

// A cached device that now carries another volume's serial is dropped and
// the volume is resolved again.
func TestResolveDropsCacheEntryWithOtherKernelSerial(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	stale := writeDisk(t, diskPath, "sdb")
	want := writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-77"))
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-42"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-42", "sdc")
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), nil)
	r.remember("vol-1", "42", "onecsi-42", stale, filepath.Join(diskPath, "disk", "by-id", "virtio-onecsi-42"))

	path, resolution, err := r.Resolve(context.Background(), "vol-1", "sdc", map[string]string{publishContextDeviceSerial: "onecsi-42"}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, want, path)
	assert.Equal(t, "by-id", resolution.ResolvedBy)
}

// Requests that carry no serial keep the alias behaviour.
func TestResolveWithoutSerialKeepsAlias(t *testing.T) {
	withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	vde := filepath.Join(diskPath, "vde")
	require.NoError(t, os.WriteFile(vde, []byte("x"), 0o644))
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), func(string) []string {
		return []string{filepath.Join(diskPath, "sde"), vde}
	})
	path, resolution, err := r.Resolve(context.Background(), "vol-1", "sde", map[string]string{}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, vde, path)
	assert.Equal(t, "alias", resolution.ResolvedBy)
}
