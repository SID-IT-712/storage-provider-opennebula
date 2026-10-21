package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

// serialToolExec answers udevadm and lsblk with fixed output. lsblkStderr is
// what a real CombinedOutput would add to lsblk's stdout.
type serialToolExec struct {
	testingexec.FakeExec
	udev        string
	lsblkStdout string
	lsblkStderr string
}

func (e *serialToolExec) Command(cmd string, args ...string) exec.Cmd {
	stdout, combined := "", ""
	switch cmd {
	case "udevadm":
		stdout, combined = e.udev, e.udev
	case "lsblk":
		stdout, combined = e.lsblkStdout, e.lsblkStdout+e.lsblkStderr
	}
	return &testingexec.FakeCmd{
		Argv:                 append([]string{cmd}, args...),
		OutputScript:         []testingexec.FakeAction{func() ([]byte, []byte, error) { return []byte(stdout), nil, nil }},
		CombinedOutputScript: []testingexec.FakeAction{func() ([]byte, []byte, error) { return []byte(combined), nil, nil }},
	}
}

func (e *serialToolExec) CommandContext(_ context.Context, cmd string, args ...string) exec.Cmd {
	return e.Command(cmd, args...)
}

// fakeBlockDeviceNumbers treats the test's regular files as block devices,
// numbered by inode, so sameBlockDevice can be exercised without real devices.
func fakeBlockDeviceNumbers(t *testing.T) {
	t.Helper()
	original := nodeBlockDeviceNumber
	nodeBlockDeviceNumber = func(path string) (uint64, bool) {
		info, err := os.Stat(path)
		if err != nil {
			return 0, false
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return 0, false
		}
		return st.Ino, true
	}
	t.Cleanup(func() { nodeBlockDeviceNumber = original })
}

// rawVPD80 is a VPD page 0x80 with an explicit declared length.
func rawVPD80(length byte, body string) []byte {
	return append([]byte{0x00, 0x80, 0x00, length}, []byte(body)...)
}

// sidLabWorker lays out the block devices measured on sid-lab worker
// talos-neb-06: virtio-scsi CSI disks with VPD 0x80 serials, Longhorn iSCSI
// disks whose 36-byte VPD serial is all spaces, sda whose serial is "40-2",
// and loop/zram/sr devices. It adds filler disks for about 20 candidates.
func sidLabWorker(t *testing.T, root, diskPath string) {
	t.Helper()
	disk := func(name string, raw []byte) {
		writeDisk(t, diskPath, name)
		if raw != nil {
			writeSysfsVPD80(t, root, name, raw)
		} else {
			require.NoError(t, os.MkdirAll(filepath.Join(root, "block", name), 0o755))
		}
	}
	disk("sda", rawVPD80(0x04, "40-2"))
	disk("sdb", rawVPD80(0x09, "onecsi-40"))
	disk("sdc", rawVPD80(0x09, "onecsi-39"))
	disk("sdd", rawVPD80(0x09, "onecsi-41"))
	disk("sdab", rawVPD80(0x24, strings.Repeat(" ", 36)))
	disk("sdag", rawVPD80(0x24, strings.Repeat(" ", 36)))
	for i, name := range []string{"sde", "sdf", "sdg", "sdh", "sdi", "sdj", "sdk", "sdl", "sdm", "sdn", "sdo", "sdp", "sdq", "sdr"} {
		disk(name, vpdPage80("onecsi-1"+string(rune('0'+i/10))+string(rune('0'+i%10))))
	}
	// Skip probes: these carry the target serial, so counting any of them
	// would make the lookup ambiguous. The real devices carry no such serial.
	for _, name := range []string{"loop0", "loop7", "zram0", "sr0", "dm-0", "ram0"} {
		disk(name, vpdPage80("onecsi-40"))
	}
	disk("sdb1", vpdPage80("onecsi-40"))
	require.NoError(t, os.WriteFile(filepath.Join(root, "block", "sdb1", "partition"), []byte("1\n"), 0o444))
}

func TestResolveFindsDiskBySysfsAmongSidLabDevices(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	sidLabWorker(t, root, diskPath)
	// no /dev/disk/by-id at all: udev lag, or no udev db in the container
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), nil)

	path, _, err := r.Resolve(context.Background(), "vol-1", "sdc", map[string]string{publishContextDeviceSerial: "onecsi-40"}, 0)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(diskPath, "sdb"), path)
}

func TestSidLabBlankAndShortSerials(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	sidLabWorker(t, root, diskPath)
	rec := newRecordingExec()
	for _, name := range []string{"sdab", "sdag"} {
		got, err := kernelDeviceSerial(filepath.Join(diskPath, name))
		require.NoError(t, err)
		assert.Equal(t, "", got, name)
		assert.Equal(t, serialUnknown, identifyDevice(rec, filepath.Join(diskPath, name), "onecsi-40"), name)
	}
	assert.Equal(t, serialMismatch, identifyDevice(rec, filepath.Join(diskPath, "sda"), "onecsi-40"))
	assert.Equal(t, serialMatch, identifyDevice(rec, filepath.Join(diskPath, "sdb"), "onecsi-40"))
}

func TestResolveRefusesAmbiguousKernelSerial(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	for _, name := range []string{"sdb", "sdc"} {
		writeDisk(t, diskPath, name)
		writeSysfsVPD80(t, root, name, vpdPage80("onecsi-11"))
	}
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), nil)
	_, _, err := r.Resolve(context.Background(), "vol-1", "sdc", map[string]string{publishContextDeviceSerial: "onecsi-11"}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestResolveSerialTimeoutNamesWhatWasSearched(t *testing.T) {
	withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdc")
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), func(string) []string {
		return []string{filepath.Join(diskPath, "sdc")}
	})
	_, _, err := r.Resolve(context.Background(), "vol-1", "sdc", map[string]string{publishContextDeviceSerial: "onecsi-11"}, 2*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carries serial onecsi-11, and no")
	assert.NotContains(t, err.Error(), filepath.Join(diskPath, "sdc"), "alias candidates must not be listed")
}

func TestKernelDeviceSerialNVMe(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	dev := writeDisk(t, diskPath, "nvme0n1")
	dir := filepath.Join(root, "block", "nvme0n1", "device")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "serial"), []byte("onecsi-5            \n"), 0o444))
	got, err := kernelDeviceSerial(dev)
	require.NoError(t, err)
	assert.Equal(t, "onecsi-5", got)
}

func TestIdentifyDeviceUdevAndLsblk(t *testing.T) {
	cases := []struct {
		name string
		exec *serialToolExec
		want serialEvidence
	}{
		{"udev ID_SERIAL only (model-prefixed) is no evidence", &serialToolExec{udev: "DEVNAME=/dev/sdb\nID_SERIAL=0QEMU_QEMU_HARDDISK_onecsi-11\n"}, serialUnknown},
		{"udev ID_SERIAL_SHORT differs", &serialToolExec{udev: "ID_SERIAL=0QEMU_QEMU_HARDDISK_onecsi-12\nID_SERIAL_SHORT=onecsi-12\n"}, serialMismatch},
		{"udev ID_SERIAL_SHORT matches", &serialToolExec{udev: "ID_SERIAL_SHORT=onecsi-11\n"}, serialMatch},
		{"udev ID_SCSI_SERIAL matches", &serialToolExec{udev: "ID_SCSI_SERIAL=onecsi-11\n"}, serialMatch},
		{"lsblk stderr only", &serialToolExec{lsblkStderr: "lsblk: /dev/sdb: not a block device\n"}, serialUnknown},
		{"lsblk stdout matches", &serialToolExec{lsblkStdout: "onecsi-11\n"}, serialMatch},
		{"lsblk stdout differs", &serialToolExec{lsblkStdout: "onecsi-12\n"}, serialMismatch},
		{"nothing known", &serialToolExec{}, serialUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withTestSysfs(t)
			diskPath := withTestDiskPath(t)
			dev := writeDisk(t, diskPath, "sdb")
			assert.Equal(t, c.want, identifyDevice(c.exec, dev, "onecsi-11"))
		})
	}
}

// A disk with no readable serial anywhere (no sysfs serial, no udev db) is
// proven by its exact by-id name, and stages.
func TestStageVolumeByIDProofWithoutAnySerialSource(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "nvme0n1")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "block", "nvme0n1", "device"), 0o755))
	linkByID(t, diskPath, "nvme-QEMU_NVMe_Ctrl_onecsi-11", "nvme0n1")
	ns, fm, _, staging := localDiskNode(t, diskPath, "")

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "nvme0n1"))
	require.NoError(t, err)
	assert.NotEmpty(t, fm.GetLog(), "the proven device is mounted")
}

func blockPublishRequest(t *testing.T, target string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: t.TempDir(),
		TargetPath:        filepath.Join(t.TempDir(), "pod", "vol-1"),
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
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

// Block publish: by-id names onecsi-11 but its device reports onecsi-99, and
// /dev/<TARGET> exists. Nothing is bound.
func TestBlockPublishRefusesWrongSerialDevice(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-99"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-11", "sdc")
	ns, fm, _, _ := localDiskNode(t, diskPath, "")

	_, err := ns.NodePublishVolume(context.Background(), blockPublishRequest(t, "sdc"))
	require.Error(t, err)
	assert.Empty(t, fm.GetLog(), "nothing may be bound")
}

func TestBlockPublishRefusesSerialRequestWithoutResolver(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-99"))
	ns, fm, _, _ := localDiskNode(t, diskPath, "")
	ns.deviceResolver = nil

	_, err := ns.NodePublishVolume(context.Background(), blockPublishRequest(t, "sdc"))
	require.Error(t, err)
	assert.Empty(t, fm.GetLog(), "nothing may be bound")
}

// Re-stage with no serial source: healthy only when the mount source is the
// same block device that the by-id proof locates.
func TestStageVolumeRestageUnknownSerialSameDevice(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	fakeBlockDeviceNumbers(t)
	writeDisk(t, diskPath, "nvme0n1")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "block", "nvme0n1"), 0o755))
	linkByID(t, diskPath, "nvme-QEMU_NVMe_Ctrl_onecsi-11", "nvme0n1")
	ns, fm, _, staging := localDiskNode(t, diskPath, "nvme0n1")

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "nvme0n1"))
	require.NoError(t, err)
	assert.Empty(t, fm.GetLog())
}

func TestStageVolumeRestageUnknownSerialOtherDevice(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	fakeBlockDeviceNumbers(t)
	for _, name := range []string{"nvme0n1", "nvme1n1"} {
		writeDisk(t, diskPath, name)
		require.NoError(t, os.MkdirAll(filepath.Join(root, "block", name), 0o755))
	}
	linkByID(t, diskPath, "nvme-QEMU_NVMe_Ctrl_onecsi-11", "nvme0n1")
	ns, fm, _, staging := localDiskNode(t, diskPath, "nvme1n1")

	_, err := ns.NodeStageVolume(context.Background(), stageRequest(staging, "nvme0n1"))
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "serial does not match")
	assert.Empty(t, fm.GetLog())
}

// The operator report and the resolver agree, also when a by-id link is stale
// (it names onecsi-11 but its disk now reports onecsi-99).
func TestReportAgreesWithResolverOnStaleByIDLink(t *testing.T) {
	root := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdb")
	writeDisk(t, diskPath, "sdc")
	writeSysfsVPD80(t, root, "sdb", vpdPage80("onecsi-99"))
	writeSysfsVPD80(t, root, "sdc", vpdPage80("onecsi-11"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-11", "sdb")
	r := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec(), nil)

	path, _, err := r.Resolve(context.Background(), "", "sdc", map[string]string{publishContextDeviceSerial: "onecsi-11"}, 0)
	require.NoError(t, err)
	_, reported := resolveByIDForReport("onecsi-11", "fallback")
	assert.Equal(t, path, reported)
	assert.Equal(t, filepath.Join(diskPath, "sdc"), reported)
}

// The SCSI rescan writes under nodeSysfsRoot, never the real /sys in tests.
func TestRecoverySequenceRescansUnderSysfsRoot(t *testing.T) {
	root := withTestSysfs(t)
	scan := filepath.Join(root, "class", "scsi_host", "host0", "scan")
	require.NoError(t, os.MkdirAll(filepath.Dir(scan), 0o755))
	require.NoError(t, os.WriteFile(scan, nil, 0o644))
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, true)
	cfg.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	r := NewNodeDeviceResolver(cfg, newRecordingExec(), nil)

	r.runRecoverySequence(context.Background())
	got, err := os.ReadFile(scan)
	require.NoError(t, err)
	assert.Equal(t, "- - -", string(got))
}
