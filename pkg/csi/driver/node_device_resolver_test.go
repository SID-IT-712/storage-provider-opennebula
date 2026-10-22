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

func TestNodeDeviceResolverPrefersByIDAndThenCache(t *testing.T) {
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
	resolver := NewNodeDeviceResolver(pluginConfig, nil, func(string) []string {
		return []string{filepath.Join(diskPath, "sde"), devicePath}
	})

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
	assert.Equal(t, "kernel-serial", resolution.ResolvedBy, "a serial request never uses the cache")
}

func TestNodeDeviceResolverInvalidatesCacheOnSerialMismatch(t *testing.T) {
	sysfs := withTestSysfs(t)
	diskPath := withTestDiskPath(t)

	oldDevicePath := filepath.Join(diskPath, "vde")
	require.NoError(t, os.WriteFile(oldDevicePath, []byte("test"), 0o644))
	newDevicePath := writeDisk(t, diskPath, "sde")
	writeSysfsVPD80(t, sysfs, "sde", vpdPage80("onecsi-43"))
	linkByID(t, diskPath, "scsi-0QEMU_QEMU_HARDDISK_onecsi-43", "sde")

	pluginConfig := config.LoadConfiguration()
	pluginConfig.OverrideVal(config.NodeDeviceRescanOnMissEnabledVar, false)
	pluginConfig.OverrideVal(config.NodeDeviceUdevSettleTimeoutSecondsVar, 0)
	resolver := NewNodeDeviceResolver(pluginConfig, nil, func(string) []string {
		return []string{newDevicePath}
	})
	resolver.remember("vol-1", "42", oldDevicePath)

	path, resolution, err := resolver.Resolve(context.Background(), "vol-1", "sde", map[string]string{
		publishContextDeviceSerial: "onecsi-43",
	}, time.Second)
	require.NoError(t, err)
	assert.Equal(t, newDevicePath, path)
	assert.Equal(t, "kernel-serial", resolution.ResolvedBy)
}
