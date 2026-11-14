package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"
)

const (
	targetPath = "/tmp/target" // Example target path for publishing
)

type failingMountInterface struct {
	*mount.FakeMounter
	err error
}

func (f *failingMountInterface) Mount(source, target, fstype string, options []string) error {
	return f.MountSensitive(source, target, fstype, options, nil)
}

func (f *failingMountInterface) MountSensitive(string, string, string, []string, []string) error {
	return f.err
}

func (f *failingMountInterface) MountSensitiveWithoutSystemd(source, target, fstype string, options []string, sensitiveOptions []string) error {
	return f.MountSensitive(source, target, fstype, options, sensitiveOptions)
}

func (f *failingMountInterface) MountSensitiveWithoutSystemdWithMountFlags(source, target, fstype string, options []string, sensitiveOptions []string, _ []string) error {
	return f.MountSensitive(source, target, fstype, options, sensitiveOptions)
}

func getTestNodeServer(mountPoints []string) *NodeServer {
	mountPointList := make([]mount.MountPoint, 0, len(mountPoints))
	for _, mountPoint := range mountPoints {
		mountPointList = append(mountPointList, mount.MountPoint{
			Path: mountPoint,
		})
	}
	return getTestNodeServerWithMountPoints(mountPointList)
}

func getTestNodeServerWithMountPoints(mountPointList []mount.MountPoint) *NodeServer {
	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	driver := &Driver{
		name:               DefaultDriverName,
		version:            driverVersion,
		grpcServerEndpoint: DefaultGRPCServerEndpoint,
		nodeID:             "test-node-id",
		maxVolumesPerNode:  30,
		PluginConfig:       pluginConfig,
		featureGates:       defaultFeatureGates(),
		metrics:            NewDriverMetrics(driverVersion, "test"),
	}
	commandScriptArray := []testingexec.FakeCommandAction{}
	//TODO: Simulate real commands
	for i := 0; i < 10; i++ {
		commandScriptArray = append(commandScriptArray, func(cmd string, args ...string) exec.Cmd {
			return &testingexec.FakeCmd{
				Argv:           append([]string{cmd}, args...),
				Stdout:         nil,
				Stderr:         nil,
				DisableScripts: true, // Disable script checking for simplicity
			}
		})
	}

	mounter := mount.NewSafeFormatAndMount(
		mount.NewFakeMounter(mountPointList), // using fake mounter implementation
		&testingexec.FakeExec{
			CommandScript: commandScriptArray,
			LookPathFunc: func(path string) (string, error) {
				return path, nil
			},
		}, // using fake exec implementation
	)
	return NewNodeServer(driver, mounter)
}

func withTestDiskPath(t *testing.T) string {
	t.Helper()

	originalDiskPath := defaultDiskPath
	originalStat := nodeVolumePathStat
	originalSleep := nodeDeviceSleep
	diskPath := t.TempDir()

	defaultDiskPath = diskPath
	nodeVolumePathStat = os.Stat
	nodeDeviceSleep = func(time.Duration) {}

	t.Cleanup(func() {
		defaultDiskPath = originalDiskPath
		nodeVolumePathStat = originalStat
		nodeDeviceSleep = originalSleep
	})

	return diskPath
}

// incidentDisks lays out the incident: OpenNebula TARGET sdad, but the guest's
// /dev/sdad is another volume's disk (serial onecsi-2690) and this volume's
// disk (serial onecsi-2936) is /dev/sdac.
func incidentDisks(t *testing.T) (diskPath, own, other string) {
	t.Helper()
	sysfs := withTestSysfs(t)
	diskPath = withTestDiskPath(t)
	steppingResolverClock(t)
	other = writeDisk(t, diskPath, "sdad")
	own = writeDisk(t, diskPath, "sdac")
	writeSysfsVPD80(t, sysfs, "sdad", vpdPage80("onecsi-2690"))
	writeSysfsVPD80(t, sysfs, "sdac", vpdPage80("onecsi-2936"))
	return diskPath, own, other
}

func incidentStageRequest(staging string, withSerial bool) *csi.NodeStageVolumeRequest {
	publishContext := map[string]string{
		"volumeName": "sdad",
		publishContextDeviceDiscoveryTimeoutSeconds: "1",
	}
	if withSerial {
		publishContext[publishContextDeviceSerial] = "onecsi-2936"
		publishContext[publishContextOpenNebulaImageID] = "2936"
	}
	return &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-2936",
		StagingTargetPath: staging,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		PublishContext: publishContext,
	}
}

func mountedAt(fm *mount.FakeMounter, device string) []string {
	paths := []string{}
	for _, mp := range fm.MountPoints {
		if mp.Device == device {
			paths = append(paths, mp.Path)
		}
	}
	return paths
}

func TestStageVolume(t *testing.T) {
	const otherStaging = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.opennebula.io/other/globalmount"
	const podBind = "/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/vol-2936/mount"

	tcs := []struct {
		name        string
		withSerial  bool
		mounts      func(own, other, staging string) []mount.MountPoint
		expectCode  codes.Code
		expectInErr []string
		// expectNamesStaging: the error names this volume's staging path too.
		expectNamesStaging bool
		// expectOwnMountedAt: where the volume's own disk is mounted afterwards.
		expectOwnMountedAt func(staging string) []string
	}{
		{
			name:       "serial proves the device: TARGET sdad is ignored, sdac is staged",
			withSerial: true,
			mounts: func(_, other, _ string) []mount.MountPoint {
				return []mount.MountPoint{{Device: other, Path: otherStaging, Type: "ext4"}}
			},
			expectCode:         codes.OK,
			expectOwnMountedAt: func(staging string) []string { return []string{staging} },
		},
		{
			name:       "incident: no serial and /dev/<TARGET> is another, mounted disk",
			withSerial: false,
			mounts: func(_, other, _ string) []mount.MountPoint {
				return []mount.MountPoint{{Device: other, Path: otherStaging, Type: "ext4"}}
			},
			expectCode:         codes.FailedPrecondition,
			expectInErr:        []string{"publish context has no deviceSerial; refusing to resolve by target name"},
			expectOwnMountedAt: func(string) []string { return []string{} },
		},
		{
			name:       "the proven device is already mounted for another volume",
			withSerial: true,
			mounts: func(own, _, _ string) []mount.MountPoint {
				return []mount.MountPoint{{Device: own, Path: otherStaging, Type: "ext4"}}
			},
			expectCode:         codes.FailedPrecondition,
			expectInErr:        []string{"already mounted at " + otherStaging, "refusing to stage it at"},
			expectNamesStaging: true,
			expectOwnMountedAt: func(string) []string { return []string{otherStaging} },
		},
		{
			name:       "idempotent: already staged at this path (and bind-published to a pod)",
			withSerial: true,
			mounts: func(own, _, staging string) []mount.MountPoint {
				return []mount.MountPoint{
					{Device: own, Path: staging, Type: "ext4"},
					{Device: own, Path: podBind, Type: "ext4"},
				}
			},
			expectCode:         codes.OK,
			expectOwnMountedAt: func(staging string) []string { return []string{staging, podBind} },
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, own, other := incidentDisks(t)
			stubStageGrow(t, false, nil, nil)
			staging := t.TempDir()
			ns := getTestNodeServerWithMountPoints(tc.mounts(own, other, staging))
			fm := ns.mounter.Interface.(*mount.FakeMounter)
			otherBefore := mountedAt(fm, other)

			response, err := ns.NodeStageVolume(context.Background(), incidentStageRequest(staging, tc.withSerial))

			assert.Equal(t, tc.expectCode, status.Code(err), "err: %v", err)
			if tc.expectCode == codes.OK {
				assert.Equal(t, &csi.NodeStageVolumeResponse{}, response)
			} else {
				assert.Nil(t, response)
			}
			if tc.expectNamesStaging {
				assert.Contains(t, err.Error(), staging)
			}
			for _, fragment := range tc.expectInErr {
				assert.Contains(t, err.Error(), fragment)
			}
			assert.ElementsMatch(t, tc.expectOwnMountedAt(staging), mountedAt(fm, own))
			assert.Equal(t, otherBefore, mountedAt(fm, other), "the other volume's disk is never mounted again")
		})
	}
}

// The mount table may spell this volume's own staging path differently (a
// symlinked kubelet dir, a trailing slash). That is still this stage, not
// "elsewhere", so a fresh stage is kept and an existing one is idempotent.
func TestRefuseDeviceMountedElsewhereMatchesStagingPathSpellings(t *testing.T) {
	_, own, _ := incidentDisks(t)
	realStaging := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "kubelet")
	assert.NoError(t, os.Symlink(realStaging, linkDir))

	for name, mountedPath := range map[string]string{
		"trailing slash": realStaging + "/",
		"resolved path":  realStaging,
	} {
		t.Run(name, func(t *testing.T) {
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Device: own, Path: mountedPath, Type: "ext4"}})
			assert.NoError(t, ns.refuseDeviceMountedElsewhere(own, linkDir, "vol-2936", ""))
		})
	}
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Device: own, Path: "/other/globalmount", Type: "ext4"}})
	assert.Equal(t, codes.FailedPrecondition, status.Code(ns.refuseDeviceMountedElsewhere(own, linkDir, "vol-2936", "")))
}

func TestMountErrorSaysDeviceMounted(t *testing.T) {
	assert.True(t, mountErrorSaysDeviceMounted(errors.New("mount: /stage: /dev/sdac already mounted or mount point busy"), "/dev/sdac"))
	assert.True(t, mountErrorSaysDeviceMounted(errors.New("fsck: /dev/sdac is mounted."), "/dev/sdac"))
	assert.False(t, mountErrorSaysDeviceMounted(errors.New("mount: /stage: filesystem is mounted read-only"), "/dev/sdac"))
	assert.False(t, mountErrorSaysDeviceMounted(errors.New("can't read superblock"), "/dev/sdac"))
	assert.False(t, mountErrorSaysDeviceMounted(nil, "/dev/sdac"))
}

// QA hole 1: kubelet passes a symlinked spelling of a staging path that is
// already staged, while a pod bind mount of this volume exists. That is the
// idempotent re-stage, not a refusal.
func TestStageVolumeIdempotentWithSymlinkedStagingAndPodBind(t *testing.T) {
	_, own, _ := incidentDisks(t)
	stubStageGrow(t, false, nil, nil)
	realStaging := t.TempDir()
	link := filepath.Join(t.TempDir(), "kubelet-link")
	require.NoError(t, os.Symlink(realStaging, link))
	podTarget := "/var/lib/kubelet/pods/uid1/volumes/kubernetes.io~csi/pv-a/mount"
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{
		{Device: own, Path: realStaging, Type: "ext4"},
		{Device: own, Path: podTarget, Type: "ext4"},
	})
	fm := ns.mounter.Interface.(*mount.FakeMounter)

	_, err := ns.NodeStageVolume(context.Background(), incidentStageRequest(link, true))

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{realStaging, podTarget}, mountedAt(fm, own), "nothing new is mounted")
}

// writeStagingDir lays out a kubelet staging dir: <dir>/globalmount plus the
// vol_data.json kubelet writes beside it, naming the volume handle.
func writeStagingDir(t *testing.T, volumeHandle string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "globalmount"), 0o755))
	if volumeHandle != "" {
		data, err := json.Marshal(map[string]string{"volumeHandle": volumeHandle, "driverName": DefaultDriverName})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "vol_data.json"), data, 0o644))
	}
	return filepath.Join(dir, "globalmount")
}

// QA hole 2 and its siblings: before refusing a device mounted elsewhere, each
// mount is attributed to a volume. This volume's own mounts are allowed;
// another volume's, and anything that cannot be attributed, are refused.
func TestStageVolumeAttributesMountsOfTheDevice(t *testing.T) {
	ownPodTarget := "/var/lib/kubelet/pods/uid1/volumes/kubernetes.io~csi/pv-a/mount"
	handlePodTarget := "/var/lib/kubelet/pods/uid1/volumes/kubernetes.io~csi/vol-2936/mount"
	otherPodTarget := "/var/lib/kubelet/pods/uid2/volumes/kubernetes.io~csi/pv-b/mount"

	tcs := []struct {
		name       string
		mountPath  func(t *testing.T) string
		expectCode codes.Code
	}{
		{"own pod bind mount (PV name from the publish context)", func(*testing.T) string { return ownPodTarget }, codes.OK},
		{"own pod bind mount (PV named like the volume handle)", func(*testing.T) string { return handlePodTarget }, codes.OK},
		{"own staging dir elsewhere (vol_data.json names this volume)", func(t *testing.T) string { return writeStagingDir(t, "vol-2936") }, codes.OK},
		{"another volume's pod bind mount", func(*testing.T) string { return otherPodTarget }, codes.FailedPrecondition},
		{"another volume's staging dir", func(t *testing.T) string { return writeStagingDir(t, "vol-2690") }, codes.FailedPrecondition},
		{"staging dir without vol_data.json (unattributable)", func(t *testing.T) string { return writeStagingDir(t, "") }, codes.FailedPrecondition},
		{"any other path (unattributable)", func(*testing.T) string { return "/mnt/manual" }, codes.FailedPrecondition},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, own, _ := incidentDisks(t)
			stubStageGrow(t, false, nil, nil)
			staging := t.TempDir()
			elsewhere := tc.mountPath(t)
			ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Device: own, Path: elsewhere, Type: "ext4"}})
			fm := ns.mounter.Interface.(*mount.FakeMounter)
			req := incidentStageRequest(staging, true)
			req.PublishContext[paramPVName] = "pv-a"

			_, err := ns.NodeStageVolume(context.Background(), req)

			assert.Equal(t, tc.expectCode, status.Code(err), "err: %v", err)
			if tc.expectCode == codes.OK {
				assert.ElementsMatch(t, []string{elsewhere, staging}, mountedAt(fm, own))
				return
			}
			assert.Contains(t, err.Error(), elsewhere)
			assert.Contains(t, err.Error(), staging)
			assert.Equal(t, []string{elsewhere}, mountedAt(fm, own), "nothing new is mounted")
		})
	}
}

// raceMounter mounts like the fake, then reports the device as also mounted
// elsewhere: what a concurrent mount (or fsck's swallowed "is mounted") means.
type raceMounter struct {
	*mount.FakeMounter
	alsoAt string
}

func (r *raceMounter) MountSensitive(source, target, fstype string, options []string, sensitive []string) error {
	if err := r.FakeMounter.MountSensitive(source, target, fstype, options, sensitive); err != nil {
		return err
	}
	r.MountPoints = append(r.MountPoints, mount.MountPoint{Device: source, Path: r.alsoAt, Type: fstype})
	return nil
}

func (r *raceMounter) MountSensitiveWithoutSystemd(source, target, fstype string, options []string, sensitive []string) error {
	return r.MountSensitive(source, target, fstype, options, sensitive)
}

func (r *raceMounter) MountSensitiveWithoutSystemdWithMountFlags(source, target, fstype string, options []string, sensitive []string, _ []string) error {
	return r.MountSensitive(source, target, fstype, options, sensitive)
}

// A device found mounted elsewhere after the mount is fatal: the new stage is
// unmounted again and NodeStage fails.
func TestStageVolumeRefusesDeviceMountedElsewhereAfterMount(t *testing.T) {
	_, own, _ := incidentDisks(t)
	stubStageGrow(t, false, nil, nil)
	staging := t.TempDir()
	base := getTestNodeServer(nil)
	fm := &raceMounter{FakeMounter: mount.NewFakeMounter(nil), alsoAt: "/elsewhere/globalmount"}
	ns := NewNodeServer(base.Driver, &mount.SafeFormatAndMount{Interface: fm, Exec: base.mounter.Exec})

	_, err := ns.NodeStageVolume(context.Background(), incidentStageRequest(staging, true))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "/elsewhere/globalmount")
	assert.NotContains(t, mountedAt(fm.FakeMounter, own), staging, "the new stage is unmounted again")
}

// FormatAndMount reporting the device as mounted is fatal and a refusal.
func TestStageVolumeTreatsDeviceIsMountedAsFatal(t *testing.T) {
	incidentDisks(t)
	stubStageGrow(t, false, nil, nil)
	staging := t.TempDir()
	base := getTestNodeServer(nil)
	fm := &failingMountInterface{FakeMounter: mount.NewFakeMounter(nil), err: errors.New("mount: " + filepath.Join(defaultDiskPath, "sdac") + " already mounted or mount point busy")}
	ns := NewNodeServer(base.Driver, &mount.SafeFormatAndMount{Interface: fm, Exec: base.mounter.Exec})

	_, err := ns.NodeStageVolume(context.Background(), incidentStageRequest(staging, true))

	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "already mounted")
}

func TestStageVolumeKeepsLocalDeviceReportUntilMountSucceeds(t *testing.T) {
	tempDir := t.TempDir()
	sysfs := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	err := os.WriteFile(filepath.Join(diskPath, "sdd"), []byte("test"), 0o644)
	assert.NoError(t, err)
	// a serial request resolves only through by-id and must prove its serial
	writeSysfsVPD80(t, sysfs, "sdd", vpdPage80("onecsi-439"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-439", "sdd")

	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.LocalDeviceRecoveryEnabledVar, true)
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	driver := &Driver{
		name:         DefaultDriverName,
		version:      driverVersion,
		nodeID:       "node-a",
		PluginConfig: pluginConfig,
		featureGates: defaultFeatureGates(),
		metrics:      NewDriverMetrics(driverVersion, "test"),
		kubeRuntime:  &KubeRuntime{client: fake.NewSimpleClientset(), enabled: true},
	}
	commandScriptArray := []testingexec.FakeCommandAction{}
	for i := 0; i < 10; i++ {
		commandScriptArray = append(commandScriptArray, func(cmd string, args ...string) exec.Cmd {
			return &testingexec.FakeCmd{
				Argv:           append([]string{cmd}, args...),
				Stdout:         nil,
				Stderr:         nil,
				DisableScripts: true,
			}
		})
	}
	mountErr := errors.New("can't read superblock")
	ns := NewNodeServer(driver, mount.NewSafeFormatAndMount(
		&failingMountInterface{FakeMounter: mount.NewFakeMounter(nil), err: mountErr},
		&testingexec.FakeExec{
			CommandScript: commandScriptArray,
			LookPathFunc: func(path string) (string, error) {
				return path, nil
			},
		},
	))
	publishContext := map[string]string{
		"volumeName":                    "sdd",
		annotationBackend:               "local",
		publishContextDeviceSerial:      "onecsi-439",
		publishContextOpenNebulaImageID: "439",
	}
	ns.recordLocalDeviceMissing(context.Background(), "vol-1", "sdd", tempDir, publishContext, errors.New("device not found"))

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: tempDir,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		PublishContext: publishContext,
	}

	_, err = ns.NodeStageVolume(context.Background(), req)
	assert.Error(t, err)
	_, err = ns.NodeStageVolume(context.Background(), req)
	assert.Error(t, err)

	cm, err := driver.kubeRuntime.client.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(context.Background(), localDeviceStateConfigMapName, metav1.GetOptions{})
	assert.NoError(t, err)
	raw := cm.Data[localDeviceReportKey("node-a", "vol-1")]
	var report LocalDeviceMissingReport
	assert.NoError(t, json.Unmarshal([]byte(raw), &report))
	assert.Equal(t, localDeviceFailureClassMountFailed, report.FailureClass)
	assert.Equal(t, 3, report.Attempts)
	assert.Equal(t, filepath.Join(diskPath, "sdd"), report.DevicePath)
}

func TestRecordLocalDiskStageSessionClearsPreviousRecoveryFailure(t *testing.T) {
	ns := getTestNodeServer(nil)
	ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
	recoveredAt := time.Now().Add(-time.Minute)
	ns.recordLocalDiskSession(localDiskSession{
		VolumeID: "vol-1",
		PublishedTargets: []localDiskPublishedTarget{
			{TargetPath: "/pods/pod-a/vol", MountOptions: []string{"rw"}},
		},
		LastRecoveredAt:   &recoveredAt,
		LastRecoveryError: "local RWO recovery attempts exhausted",
		RecoveryAttempts:  3,
	})

	ns.recordLocalDiskStageSession(&csi.NodeStageVolumeRequest{
		VolumeId:          "vol-1",
		StagingTargetPath: "/stage/vol-1",
		PublishContext: map[string]string{
			"volumeName": "sdg",
		},
	}, "/dev/sdg", "xfs", []string{"rw"})

	session, exists, err := ns.loadLocalDiskSession("vol-1")
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Len(t, session.PublishedTargets, 1)
	assert.Nil(t, session.LastRecoveredAt)
	assert.Empty(t, session.LastRecoveryError)
	assert.Equal(t, 0, session.RecoveryAttempts)
}

func TestResolveDevicePath(t *testing.T) {
	t.Run("refuses a context without deviceSerial although /dev/<TARGET> exists", func(t *testing.T) {
		_, _, _ = incidentDisks(t)
		ns := getTestNodeServer(nil)

		devicePath, resolution, resolveErr := ns.resolveDevicePathWithContext(context.Background(), "vol-2936", "sdad", map[string]string{"volumeName": "sdad"}, time.Second)

		assert.Empty(t, devicePath)
		assert.Empty(t, resolution.ResolvedBy)
		assert.ErrorIs(t, resolveErr, errNoDeviceSerial)
		assert.Equal(t, codes.FailedPrecondition, deviceResolutionFailureCode(resolveErr))
	})

	t.Run("refuses a context without deviceSerial when there is no resolver", func(t *testing.T) {
		_, _, _ = incidentDisks(t)
		ns := getTestNodeServer(nil)
		ns.deviceResolver = nil

		devicePath, _, resolveErr := ns.resolveDevicePathWithContext(context.Background(), "vol-2936", "sdad", map[string]string{"volumeName": "sdad"}, time.Second)

		assert.Empty(t, devicePath)
		assert.ErrorIs(t, resolveErr, errNoDeviceSerial)
		assert.Contains(t, resolveErr.Error(), "publish context has no deviceSerial; refusing to resolve by target name")
		assert.Equal(t, codes.FailedPrecondition, deviceResolutionFailureCode(resolveErr))
	})

	t.Run("resolves by serial, not by target name", func(t *testing.T) {
		_, own, _ := incidentDisks(t)
		ns := getTestNodeServer(nil)

		devicePath, resolution, resolveErr := ns.resolveDevicePathWithContext(context.Background(), "vol-2936", "sdad", map[string]string{
			"volumeName":               "sdad",
			publishContextDeviceSerial: "onecsi-2936",
		}, time.Second)

		require.NoError(t, resolveErr)
		assert.Equal(t, own, devicePath)
		assert.Equal(t, "kernel-serial", resolution.ResolvedBy)
	})

	t.Run("times out when no device carries the serial", func(t *testing.T) {
		withTestSysfs(t)
		withTestDiskPath(t)
		steppingResolverClock(t)
		ns := getTestNodeServer(nil)
		ns.Driver.PluginConfig.OverrideVal(config.NodeDeviceDiscoveryTimeoutVar, 1)

		devicePath, _, resolveErr := ns.resolveDevicePathWithContext(context.Background(), "vol-1", "sdf", map[string]string{publishContextDeviceSerial: "onecsi-5"}, ns.deviceDiscoveryTimeout(nil))

		assert.Empty(t, devicePath)
		require.Error(t, resolveErr)
		assert.Contains(t, resolveErr.Error(), "after 1s")
		assert.Contains(t, resolveErr.Error(), "TARGET sdf is not used")
		assert.Equal(t, codes.DeadlineExceeded, deviceResolutionFailureCode(resolveErr))
	})

	t.Run("uses dedicated publish context timeout override when present", func(t *testing.T) {
		ns := getTestNodeServer(nil)
		ns.Driver.PluginConfig.OverrideVal(config.NodeDeviceDiscoveryTimeoutVar, 30)

		timeout := ns.deviceDiscoveryTimeout(map[string]string{
			publishContextDeviceDiscoveryTimeoutSeconds: "30",
			publishContextHotplugTimeoutSeconds:         "300",
		})

		assert.Equal(t, 30*time.Second, timeout)
	})

	t.Run("falls back to configured device discovery timeout", func(t *testing.T) {
		ns := getTestNodeServer(nil)
		ns.Driver.PluginConfig.OverrideVal(config.NodeDeviceDiscoveryTimeoutVar, 45)

		timeout := ns.deviceDiscoveryTimeout(nil)

		assert.Equal(t, 45*time.Second, timeout)
	})
}

func TestStageSharedFilesystemVolume(t *testing.T) {
	tempDir := t.TempDir()
	ns := getTestNodeServer([]string{})

	resp, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoic3RhdGljIiwic3VicGF0aCI6Ii9rdWJlcm5ldGVzL3N0YXRpYy9tb2RlbC1jYWNoZSJ9",
		StagingTargetPath: tempDir,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		PublishContext: map[string]string{
			"shareBackend":   "cephfs",
			"cephfsMonitors": "mon1,mon2",
			"cephfsFSName":   "cephfs-prod",
			"cephfsSubpath":  "/kubernetes/static/model-cache",
			"cephfsReadonly": "false",
		},
		Secrets: map[string]string{
			"userID":  "csi-node",
			"userKey": "super-secret",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, &csi.NodeStageVolumeResponse{}, resp)
}

func TestUnstageVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeUnstageVolumeRequest
		expectResponse *csi.NodeUnstageVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: tempDir,
			},
			expectResponse: &csi.NodeUnstageVolumeResponse{},
			expectError:    false,
		},
		{
			name: "[SUCCESS] Test unmounted staging target path",
			request: &csi.NodeUnstageVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: "/tmp/nonexistent",
			},
			expectResponse: &csi.NodeUnstageVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeUnstageVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestPublishVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodePublishVolumeRequest
		expectResponse *csi.NodePublishVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodePublishVolumeRequest{
				VolumeId:          "test-volume-id",
				StagingTargetPath: tempDir,
				TargetPath:        targetPath,
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{
							FsType:     "ext4",
							MountFlags: []string{"ro"},
						},
					},
				},
				PublishContext: map[string]string{
					"volumeName": "zero",
				},
			},
			expectResponse: &csi.NodePublishVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodePublishVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestPublishSharedFilesystemVolume(t *testing.T) {
	tempDir := t.TempDir()
	ns := getTestNodeServer([]string{tempDir})

	resp, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "cephfs:eyJiYWNrZW5kIjoiY2VwaGZzIiwiZGF0YXN0b3JlSUQiOjMwMCwiZnNOYW1lIjoiY2VwaGZzLXByb2QiLCJtb2RlIjoiZHluYW1pYyIsInN1YnBhdGgiOiIva3ViZXJuZXRlcy9keW5hbWljL29uZS1jc2ktZGVtbyJ9",
		StagingTargetPath: tempDir,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
		PublishContext: map[string]string{
			"shareBackend":   "cephfs",
			"cephfsMonitors": "mon1,mon2",
			"cephfsFSName":   "cephfs-prod",
			"cephfsSubpath":  "/kubernetes/dynamic/one-csi-demo",
			"cephfsReadonly": "false",
		},
	})

	assert.NoError(t, err)
	assert.Equal(t, &csi.NodePublishVolumeResponse{}, resp)
}

func TestUnpublishVolume(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeUnpublishVolumeRequest
		expectResponse *csi.NodeUnpublishVolumeResponse
		expectError    bool
	}{
		{
			name: "[SUCCESS] Test correct staging target path",
			request: &csi.NodeUnpublishVolumeRequest{
				VolumeId:   "test-volume-id",
				TargetPath: targetPath,
			},
			expectResponse: &csi.NodeUnpublishVolumeResponse{},
			expectError:    false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeUnpublishVolume(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	t.Run("returns filesystem stats for mounted directory", func(t *testing.T) {
		tempDir := t.TempDir()
		ns := getTestNodeServer([]string{tempDir})

		response, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "test-volume-id",
			VolumePath: tempDir,
		})

		assert.NoError(t, err)
		if assert.Len(t, response.GetUsage(), 2) {
			assert.Equal(t, csi.VolumeUsage_BYTES, response.GetUsage()[0].GetUnit())
			assert.Greater(t, response.GetUsage()[0].GetTotal(), int64(0))
			assert.Equal(t, csi.VolumeUsage_INODES, response.GetUsage()[1].GetUnit())
			assert.Greater(t, response.GetUsage()[1].GetTotal(), int64(0))
		}
	})

	t.Run("returns byte stats for block-like file", func(t *testing.T) {
		tempDir := t.TempDir()
		filePath := tempDir + "/volume.img"
		payload := make([]byte, 4096)
		err := os.WriteFile(filePath, payload, 0o644)
		assert.NoError(t, err)

		ns := getTestNodeServer([]string{tempDir})
		response, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "test-volume-id",
			VolumePath: filePath,
		})

		assert.NoError(t, err)
		if assert.Len(t, response.GetUsage(), 1) {
			assert.Equal(t, csi.VolumeUsage_BYTES, response.GetUsage()[0].GetUnit())
			assert.Equal(t, int64(len(payload)), response.GetUsage()[0].GetTotal())
			assert.Equal(t, int64(len(payload)), response.GetUsage()[0].GetUsed())
		}
	})

	t.Run("rejects empty volume path", func(t *testing.T) {
		ns := getTestNodeServer(nil)
		response, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId: "test-volume-id",
		})

		assert.Error(t, err)
		assert.Nil(t, response)
	})

	t.Run("maps disconnected cephfs statfs to failed precondition", func(t *testing.T) {
		tempDir := t.TempDir()
		ns := getTestNodeServer([]string{tempDir})

		originalStat := nodeVolumePathStat
		originalStatfs := nodeVolumePathFS
		t.Cleanup(func() {
			nodeVolumePathStat = originalStat
			nodeVolumePathFS = originalStatfs
		})

		nodeVolumePathStat = func(name string) (os.FileInfo, error) {
			return originalStat(name)
		}
		nodeVolumePathFS = func(path string, buf *unix.Statfs_t) error {
			return errors.New("transport endpoint is not connected")
		}

		response, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "cephfs:test-volume-id",
			VolumePath: tempDir,
		})

		assert.Nil(t, response)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "stale CephFS mount detected")
	})

	t.Run("maps stale local disk statfs to failed precondition", func(t *testing.T) {
		tempDir := t.TempDir()
		ns := getTestNodeServer([]string{tempDir})
		ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
		ns.recordLocalDiskSession(localDiskSession{
			VolumeID:          "test-volume-id",
			StagingTargetPath: tempDir,
			PVCNamespace:      "default",
			PVCName:           "claim",
		})

		originalStat := nodeVolumePathStat
		originalStatfs := nodeVolumePathFS
		t.Cleanup(func() {
			nodeVolumePathStat = originalStat
			nodeVolumePathFS = originalStatfs
		})

		nodeVolumePathStat = func(name string) (os.FileInfo, error) {
			return originalStat(name)
		}
		nodeVolumePathFS = func(path string, buf *unix.Statfs_t) error {
			return unix.EIO
		}

		response, err := ns.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "test-volume-id",
			VolumePath: tempDir,
		})

		assert.Nil(t, response)
		assert.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "stale local disk mount detected")
	})
}

func TestNodeExpandVolume(t *testing.T) {
	const requiredBytes = int64(2 * 1024 * 1024 * 1024)

	setFilesystemBytesSequence := func(sequence []int64) {
		index := 0
		nodeVolumePathFS = func(_ string, buf *unix.Statfs_t) error {
			sizeBytes := sequence[len(sequence)-1]
			if index < len(sequence) {
				sizeBytes = sequence[index]
				index++
			}
			buf.Bsize = 4096
			buf.Blocks = uint64(sizeBytes / int64(buf.Bsize))
			buf.Bavail = buf.Blocks
			buf.Bfree = buf.Blocks
			buf.Files = 1024
			buf.Ffree = 1024
			return nil
		}
	}

	makeFilesystemRequest := func(volumePath string) *csi.NodeExpandVolumeRequest {
		return &csi.NodeExpandVolumeRequest{
			VolumeId:          "test-volume-id",
			VolumePath:        volumePath,
			StagingTargetPath: volumePath,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: requiredBytes,
			},
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "xfs"},
				},
			},
		}
	}

	t.Run("returns immediately for block volume", func(t *testing.T) {
		tempDir := t.TempDir()
		ns := getTestNodeServer([]string{tempDir})
		response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
			VolumeId:   "test-volume-id",
			VolumePath: tempDir,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: requiredBytes,
			},
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Block{
					Block: &csi.VolumeCapability_BlockVolume{},
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, &csi.NodeExpandVolumeResponse{CapacityBytes: requiredBytes}, response)
	})

	t.Run("retries until filesystem reaches target", func(t *testing.T) {
		originalStatfs := nodeVolumePathFS
		originalSleep := nodeDeviceSleep
		originalNow := nodeNow
		originalResizeFS := nodeResizeFS
		originalGOOS := nodeRuntimeGOOS
		t.Cleanup(func() {
			nodeVolumePathFS = originalStatfs
			nodeDeviceSleep = originalSleep
			nodeNow = originalNow
			nodeResizeFS = originalResizeFS
			nodeRuntimeGOOS = originalGOOS
		})

		volumePath := t.TempDir()
		devicePath := filepath.Join(t.TempDir(), "device")
		assert.NoError(t, os.WriteFile(devicePath, []byte("x"), 0o644))
		assert.NoError(t, os.Truncate(devicePath, requiredBytes))

		ns := getTestNodeServerWithMountPoints([]mount.MountPoint{
			{Path: volumePath, Device: devicePath},
		})
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 30)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 2)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandSizeToleranceBytesVar, 0)

		nodeRuntimeGOOS = "linux"
		now := time.Unix(0, 0)
		nodeNow = func() time.Time { return now }
		nodeDeviceSleep = func(d time.Duration) { now = now.Add(d) }

		resizeCalls := 0
		nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
			resizeCalls++
			return true, nil
		}
		setFilesystemBytesSequence([]int64{
			requiredBytes / 2,
			requiredBytes / 2,
			requiredBytes,
		})

		response, err := ns.NodeExpandVolume(context.Background(), makeFilesystemRequest(volumePath))
		assert.NoError(t, err)
		assert.Equal(t, &csi.NodeExpandVolumeResponse{CapacityBytes: requiredBytes}, response)
		assert.Equal(t, 3, resizeCalls)
	})

	t.Run("returns deadline exceeded when filesystem never converges", func(t *testing.T) {
		originalStatfs := nodeVolumePathFS
		originalSleep := nodeDeviceSleep
		originalNow := nodeNow
		originalResizeFS := nodeResizeFS
		originalGOOS := nodeRuntimeGOOS
		t.Cleanup(func() {
			nodeVolumePathFS = originalStatfs
			nodeDeviceSleep = originalSleep
			nodeNow = originalNow
			nodeResizeFS = originalResizeFS
			nodeRuntimeGOOS = originalGOOS
		})

		volumePath := t.TempDir()
		devicePath := filepath.Join(t.TempDir(), "device")
		assert.NoError(t, os.WriteFile(devicePath, []byte("x"), 0o644))
		assert.NoError(t, os.Truncate(devicePath, requiredBytes))

		ns := getTestNodeServerWithMountPoints([]mount.MountPoint{
			{Path: volumePath, Device: devicePath},
		})
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 5)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandSizeToleranceBytesVar, 0)

		nodeRuntimeGOOS = "linux"
		now := time.Unix(0, 0)
		nodeNow = func() time.Time { return now }
		nodeDeviceSleep = func(d time.Duration) { now = now.Add(d) }

		resizeCalls := 0
		nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
			resizeCalls++
			return true, nil
		}
		setFilesystemBytesSequence([]int64{requiredBytes / 2})

		response, err := ns.NodeExpandVolume(context.Background(), makeFilesystemRequest(volumePath))
		assert.Nil(t, response)
		assert.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		assert.Contains(t, err.Error(), "requested_bytes")
		assert.GreaterOrEqual(t, resizeCalls, 2)
	})

	t.Run("waits for device expansion before resizing filesystem", func(t *testing.T) {
		originalStatfs := nodeVolumePathFS
		originalSleep := nodeDeviceSleep
		originalNow := nodeNow
		originalResizeFS := nodeResizeFS
		originalGOOS := nodeRuntimeGOOS
		t.Cleanup(func() {
			nodeVolumePathFS = originalStatfs
			nodeDeviceSleep = originalSleep
			nodeNow = originalNow
			nodeResizeFS = originalResizeFS
			nodeRuntimeGOOS = originalGOOS
		})

		volumePath := t.TempDir()
		devicePath := filepath.Join(t.TempDir(), "device")
		assert.NoError(t, os.WriteFile(devicePath, []byte("x"), 0o644))
		assert.NoError(t, os.Truncate(devicePath, requiredBytes/2))

		ns := getTestNodeServerWithMountPoints([]mount.MountPoint{
			{Path: volumePath, Device: devicePath},
		})
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 20)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 2)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandSizeToleranceBytesVar, 0)

		nodeRuntimeGOOS = "linux"
		now := time.Unix(0, 0)
		nodeNow = func() time.Time { return now }

		sleepCalls := 0
		nodeDeviceSleep = func(d time.Duration) {
			sleepCalls++
			now = now.Add(d)
			if sleepCalls == 1 {
				_ = os.Truncate(devicePath, requiredBytes)
			}
		}

		resizeCalls := 0
		nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
			resizeCalls++
			return true, nil
		}
		setFilesystemBytesSequence([]int64{requiredBytes})

		response, err := ns.NodeExpandVolume(context.Background(), makeFilesystemRequest(volumePath))
		assert.NoError(t, err)
		assert.Equal(t, &csi.NodeExpandVolumeResponse{CapacityBytes: requiredBytes}, response)
		assert.Equal(t, 1, sleepCalls)
		assert.Equal(t, 1, resizeCalls)
	})

	t.Run("accepts ext4 filesystem overhead after successful resize", func(t *testing.T) {
		originalStatfs := nodeVolumePathFS
		originalSleep := nodeDeviceSleep
		originalNow := nodeNow
		originalResizeFS := nodeResizeFS
		originalGOOS := nodeRuntimeGOOS
		t.Cleanup(func() {
			nodeVolumePathFS = originalStatfs
			nodeDeviceSleep = originalSleep
			nodeNow = originalNow
			nodeResizeFS = originalResizeFS
			nodeRuntimeGOOS = originalGOOS
		})

		const required4Gi = int64(4 * 1024 * 1024 * 1024)
		const ext4FilesystemBytes = int64(4152066048)

		volumePath := t.TempDir()
		devicePath := filepath.Join(t.TempDir(), "device")
		assert.NoError(t, os.WriteFile(devicePath, []byte("x"), 0o644))
		assert.NoError(t, os.Truncate(devicePath, required4Gi))

		ns := getTestNodeServerWithMountPoints([]mount.MountPoint{
			{Path: volumePath, Device: devicePath},
		})
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandVerifyTimeoutSecondsVar, 10)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandRetryIntervalSecondsVar, 2)
		ns.Driver.PluginConfig.OverrideVal(config.NodeExpandSizeToleranceBytesVar, 128*1024*1024)

		nodeRuntimeGOOS = "linux"
		now := time.Unix(0, 0)
		nodeNow = func() time.Time { return now }
		nodeDeviceSleep = func(d time.Duration) { now = now.Add(d) }

		resizeCalls := 0
		nodeResizeFS = func(_ exec.Interface, _, _ string) (bool, error) {
			resizeCalls++
			return true, nil
		}
		setFilesystemBytesSequence([]int64{ext4FilesystemBytes})

		response, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
			VolumeId:          "test-volume-id",
			VolumePath:        volumePath,
			StagingTargetPath: volumePath,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: required4Gi,
			},
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
				},
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, &csi.NodeExpandVolumeResponse{CapacityBytes: required4Gi}, response)
		assert.Equal(t, 1, resizeCalls)
	})
}

func TestNodeGetCapabilities(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeGetCapabilitiesRequest
		expectResponse *csi.NodeGetCapabilitiesResponse
		expectError    bool
	}{
		{
			name:    "[Success] Test capabilities",
			request: &csi.NodeGetCapabilitiesRequest{},
			expectResponse: &csi.NodeGetCapabilitiesResponse{
				Capabilities: []*csi.NodeServiceCapability{
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
							},
						},
					},
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
							},
						},
					},
					{
						Type: &csi.NodeServiceCapability_Rpc{
							Rpc: &csi.NodeServiceCapability_RPC{
								Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
							},
						},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeGetCapabilities(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetInfo(t *testing.T) {
	tempDir := t.TempDir()
	tcs := []struct {
		name           string
		request        *csi.NodeGetInfoRequest
		expectResponse *csi.NodeGetInfoResponse
		expectError    bool
	}{
		{
			name:    "[Success] Test retrieved node info",
			request: &csi.NodeGetInfoRequest{},
			expectResponse: &csi.NodeGetInfoResponse{
				NodeId:            "test-node-id",
				MaxVolumesPerNode: 30,
			},
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ns := getTestNodeServer([]string{tempDir})
			response, err := ns.NodeGetInfo(context.Background(), tc.request)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectResponse != nil {
				assert.Equal(t, tc.expectResponse, response)
			} else {
				assert.NotNil(t, response)
			}
		})
	}
}

func TestNodeGetInfoIncludesAccessibleTopologyWhenEnabled(t *testing.T) {
	tempDir := t.TempDir()
	ns := getTestNodeServer([]string{tempDir})
	ns.Driver.featureGates.TopologyAccessibility = true
	ns.Driver.PluginConfig.OverrideVal(config.NodeTopologySystemDSVar, "111")

	response, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	assert.NoError(t, err)
	if assert.NotNil(t, response.GetAccessibleTopology()) {
		assert.Equal(t, "111", response.GetAccessibleTopology().Segments[topologySystemDSLabel])
	}
}
