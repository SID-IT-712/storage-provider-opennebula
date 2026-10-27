package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

// stageGrowCalls records what NodeStageVolume asked the filesystem resizer.
type stageGrowCalls struct {
	needResize [][2]string // {devicePath, mountPath}
	resize     [][2]string
}

// stubStageGrow replaces the mount-utils resizer seams for one test. needResize
// is what NeedResize reports (a filesystem smaller than its device when true).
// The real resizer shells out to blockdev/blkid/dumpe2fs, which the fake execs
// in these tests do not script.
func stubStageGrow(t *testing.T, needResize bool, needErr, resizeErr error) *stageGrowCalls {
	t.Helper()
	calls := &stageGrowCalls{}
	origNeed, origResize, origGOOS := nodeNeedResizeFS, nodeResizeFS, nodeRuntimeGOOS
	t.Cleanup(func() {
		nodeNeedResizeFS, nodeResizeFS, nodeRuntimeGOOS = origNeed, origResize, origGOOS
	})
	nodeRuntimeGOOS = "linux"
	nodeNeedResizeFS = func(_ exec.Interface, devicePath, mountPath string) (bool, error) {
		calls.needResize = append(calls.needResize, [2]string{devicePath, mountPath})
		return needResize, needErr
	}
	nodeResizeFS = func(_ exec.Interface, devicePath, mountPath string) (bool, error) {
		calls.resize = append(calls.resize, [2]string{devicePath, mountPath})
		return resizeErr == nil, resizeErr
	}
	return calls
}

func stageGrowRequest(staging string, capability *csi.VolumeCapability) *csi.NodeStageVolumeRequest {
	return &csi.NodeStageVolumeRequest{
		VolumeId:          "vol-grow",
		StagingTargetPath: staging,
		VolumeCapability:  capability,
		PublishContext:    map[string]string{"volumeName": "zero"},
	}
}

func mountCapability(fsType string, mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: fsType}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

func stageGrowDisk(t *testing.T) string {
	t.Helper()
	diskPath := withTestDiskPath(t)
	require.NoError(t, os.WriteFile(filepath.Join(diskPath, "zero"), []byte("test"), 0o644))
	return filepath.Join(diskPath, "zero")
}

// A clone requested larger than its source arrives with the source's smaller
// filesystem on a grown device. Kubernetes never calls NodeExpandVolume for a
// fresh volume, so NodeStage must grow it (lamp-lab01, ops case 80 Phase F).
func TestStageVolumeGrowsFilesystemSmallerThanDevice(t *testing.T) {
	for _, fsType := range []string{"ext4", "xfs"} {
		t.Run(fsType, func(t *testing.T) {
			device := stageGrowDisk(t)
			calls := stubStageGrow(t, true, nil, nil)
			staging := t.TempDir()
			ns := getTestNodeServer(nil)

			_, err := ns.NodeStageVolume(context.Background(),
				stageGrowRequest(staging, mountCapability(fsType, csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)))
			require.NoError(t, err)
			assert.Equal(t, [][2]string{{device, staging}}, calls.needResize)
			// xfs_growfs works on the mount point, resize2fs on the device; the
			// resizer gets both and picks.
			assert.Equal(t, [][2]string{{device, staging}}, calls.resize)
		})
	}
}

func TestStageVolumeLeavesFilesystemThatFillsDevice(t *testing.T) {
	stageGrowDisk(t)
	calls := stubStageGrow(t, false, nil, nil)
	ns := getTestNodeServer(nil)

	_, err := ns.NodeStageVolume(context.Background(),
		stageGrowRequest(t.TempDir(), mountCapability("ext4", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)))
	require.NoError(t, err)
	assert.Len(t, calls.needResize, 1)
	assert.Empty(t, calls.resize)
}

// A grow that fails must fail the stage, never mount a smaller filesystem and
// report success. kubelet retries NodeStage on Internal.
func TestStageVolumeFailsWhenGrowFails(t *testing.T) {
	stageGrowDisk(t)
	stubStageGrow(t, true, nil, errors.New("resize2fs: exit status 1"))
	ns := getTestNodeServer(nil)

	_, err := ns.NodeStageVolume(context.Background(),
		stageGrowRequest(t.TempDir(), mountCapability("ext4", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "resize2fs: exit status 1")
}

func TestStageVolumeFailsWhenGrowCheckFails(t *testing.T) {
	stageGrowDisk(t)
	calls := stubStageGrow(t, false, errors.New("blockdev: not found"), nil)
	ns := getTestNodeServer(nil)

	_, err := ns.NodeStageVolume(context.Background(),
		stageGrowRequest(t.TempDir(), mountCapability("ext4", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Empty(t, calls.resize)
}

// The retry after a failed grow finds the stage already mounted. That branch
// must re-check the grow too, or the retry reports success on a small filesystem.
func TestStageVolumeRestageGrowsAlreadyMountedFilesystem(t *testing.T) {
	device := stageGrowDisk(t)
	calls := stubStageGrow(t, true, nil, nil)
	staging := t.TempDir()
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: staging, Device: device, Type: "ext4"}})

	_, err := ns.NodeStageVolume(context.Background(),
		stageGrowRequest(staging, mountCapability("ext4", csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)))
	require.NoError(t, err)
	assert.Equal(t, [][2]string{{device, staging}}, calls.resize)
}

func TestStageVolumeBlockNeverGrows(t *testing.T) {
	stageGrowDisk(t)
	calls := stubStageGrow(t, true, nil, nil)
	ns := getTestNodeServer(nil)
	capability := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}

	_, err := ns.NodeStageVolume(context.Background(), stageGrowRequest(t.TempDir(), capability))
	require.NoError(t, err)
	assert.Empty(t, calls.needResize)
	assert.Empty(t, calls.resize)
}

// A read-only mount cannot be grown online (EROFS); it is left alone. (The fake
// exec reports every disk unformatted, which a read-only stage refuses to format,
// so the staging path starts mounted.)
func TestStageVolumeReadOnlyNeverGrows(t *testing.T) {
	device := stageGrowDisk(t)
	calls := stubStageGrow(t, true, nil, nil)
	staging := t.TempDir()
	ns := getTestNodeServerWithMountPoints([]mount.MountPoint{{Path: staging, Device: device, Type: "ext4", Opts: []string{"ro"}}})

	_, err := ns.NodeStageVolume(context.Background(),
		stageGrowRequest(staging, mountCapability("ext4", csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY)))
	require.NoError(t, err)
	assert.Empty(t, calls.needResize)
	assert.Empty(t, calls.resize)
}

// Local RWO stale-mount recovery re-stages with its own FormatAndMount, so it
// grows the filesystem the same way.
func TestLocalDiskRecoveryGrowsFilesystemSmallerThanDevice(t *testing.T) {
	device := stageGrowDisk(t)
	calls := stubStageGrow(t, true, nil, nil)
	staging := t.TempDir()
	ns := getTestNodeServer(nil)
	ns.Driver.featureGates.LocalRWOStaleMountRecovery = true
	ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
	session := localDiskSession{VolumeID: "vol-grow", VolumeName: "zero", StagingTargetPath: staging, FSType: "ext4"}

	require.NoError(t, ns.recoverLocalDiskSession(context.Background(), session, "test"))
	assert.Equal(t, [][2]string{{device, staging}}, calls.resize)
}

func TestLocalDiskRecoveryFailsWhenGrowFails(t *testing.T) {
	stageGrowDisk(t)
	stubStageGrow(t, true, nil, errors.New("xfs_growfs: exit status 1"))
	ns := getTestNodeServer(nil)
	ns.Driver.featureGates.LocalRWOStaleMountRecovery = true
	ns.localDiskSessions = newLocalDiskSessionStore(t.TempDir())
	session := localDiskSession{VolumeID: "vol-grow", VolumeName: "zero", StagingTargetPath: t.TempDir(), FSType: "xfs"}

	err := ns.recoverLocalDiskSession(context.Background(), session, "test")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "xfs_growfs: exit status 1")
}
