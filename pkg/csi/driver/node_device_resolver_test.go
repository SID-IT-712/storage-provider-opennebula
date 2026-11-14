package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeDeviceResolverProvesSerialOnEveryCall(t *testing.T) {
	sysfs := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	writeSysfsSerial(t, sysfs, "vde", "onecsi-42")
	require.NoError(t, os.MkdirAll(filepath.Join(diskPath, "disk", "by-id"), 0o755))
	devicePath := filepath.Join(diskPath, "vde")
	require.NoError(t, os.WriteFile(devicePath, []byte("test"), 0o644))
	byIDPath := filepath.Join(diskPath, "disk", "by-id", "virtio-onecsi-42")
	require.NoError(t, os.Symlink(devicePath, byIDPath))
	resolvedDevicePath, err := filepath.EvalSymlinks(devicePath)
	require.NoError(t, err)

	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	resolver := NewNodeDeviceResolver(pluginConfig, nil)

	path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial:      "onecsi-42",
		publishContextOpenNebulaImageID: "42",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, resolvedDevicePath, path)
	assert.Equal(t, "kernel-serial", resolution.ResolvedBy)

	require.NoError(t, os.Remove(byIDPath))
	path, resolution, err = resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial:      "onecsi-42",
		publishContextOpenNebulaImageID: "42",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, resolvedDevicePath, path)
	assert.Equal(t, "kernel-serial", resolution.ResolvedBy, "every call is proven again; nothing is cached")
}

// The incident: a publish context with no deviceSerial and TARGET sdad, where
// /dev/sdad exists but is another volume's disk (serial onecsi-2690). The
// resolver used to return it as "exact". It must refuse, with or without an
// image id, and never fall back to the target name.
func TestNodeDeviceResolverRefusesContextWithoutSerial(t *testing.T) {
	sysfs := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdad")
	writeSysfsVPD80(t, sysfs, "sdad", vpdPage80("onecsi-2690"))
	resolver := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec())

	for name, publishContext := range map[string]map[string]string{
		"no serial":              {"volumeName": "sdad"},
		"image id but no serial": {"volumeName": "sdad", publishContextOpenNebulaImageID: "2936"},
		"blank serial":           {"volumeName": "sdad", publishContextDeviceSerial: "  "},
	} {
		t.Run(name, func(t *testing.T) {
			path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sdad", publishContext, time.Second)
			require.Error(t, err)
			assert.ErrorIs(t, err, errNoDeviceSerial)
			assert.Contains(t, err.Error(), "publish context has no deviceSerial; refusing to resolve by target name")
			assert.Empty(t, path)
			assert.Empty(t, resolution.ResolvedBy)
		})
	}
}

// The incident with a serial: TARGET sdad names another volume's disk; the
// serial onecsi-2936 is on sdac. The serial decides, never the target name.
func TestNodeDeviceResolverIncidentResolvesBySerialNotTarget(t *testing.T) {
	sysfs := withTestSysfs(t)
	diskPath := withTestDiskPath(t)
	steppingResolverClock(t)
	writeDisk(t, diskPath, "sdad")
	want := writeDisk(t, diskPath, "sdac")
	writeSysfsVPD80(t, sysfs, "sdad", vpdPage80("onecsi-2690"))
	writeSysfsVPD80(t, sysfs, "sdac", vpdPage80("onecsi-2936"))
	resolver := NewNodeDeviceResolver(getTestNodeServer(nil).Driver.PluginConfig, newRecordingExec())

	path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sdad", map[string]string{
		"volumeName":                    "sdad",
		publishContextDeviceSerial:      "onecsi-2936",
		publishContextOpenNebulaImageID: "2936",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, want, path)
	assert.Equal(t, "kernel-serial", resolution.ResolvedBy)
	assert.Equal(t, proofKernel, resolution.Proof)
}
