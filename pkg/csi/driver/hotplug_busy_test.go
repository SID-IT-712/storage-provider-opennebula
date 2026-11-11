package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// diagnosingProvider adds a fixed hotplug diagnosis to the testify mock, so
// handleHotplugTimeout sees what OpenNebula reported about the VM.
type diagnosingProvider struct {
	*MockOpenNebulaVolumeProviderTestify
	diagnosis opennebula.HotplugDiagnosis
}

func (p *diagnosingProvider) DiagnoseNodeHotplug(_ context.Context, node string, _ *opennebula.HotplugObservation, _ opennebula.HotplugDiagnosisConfig) (opennebula.HotplugDiagnosis, opennebula.HotplugObservation, error) {
	return p.diagnosis, opennebula.HotplugObservation{Node: node}, nil
}

func newBusyTestServer(t *testing.T, diagnosis opennebula.HotplugDiagnosis, nodeReadyCalls int) (*ControllerServer, *Driver, *MockOpenNebulaVolumeProviderTestify) {
	t.Helper()
	driver := newStickyTestDriver(t, newReadyNode("node-1", true))
	driver.PluginConfig.OverrideVal(config.NodeHotplugGuardEnabledVar, true)
	driver.PluginConfig.OverrideVal(config.NodeHotplugGuardFailureThresholdVar, 3)
	driver.PluginConfig.OverrideVal(config.NodeHotplugGuardRequireKubernetesReadyVar, false)
	driver.PluginConfig.OverrideVal(config.NodeHotplugGuardRequireOpenNebulaReadyVar, true)
	driver.PluginConfig.OverrideVal(config.HotplugDiagnosticsEnabledVar, true)

	mockProvider := new(MockOpenNebulaVolumeProviderTestify)
	if nodeReadyCalls > 0 {
		// The VM is in HOTPLUG, so OpenNebula reports it as not RUNNING.
		mockProvider.On("NodeReady", mock.Anything, "node-1").Return(false, nil).Times(nodeReadyCalls)
	}
	provider := &diagnosingProvider{MockOpenNebulaVolumeProviderTestify: mockProvider, diagnosis: diagnosis}
	return NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{}), driver, mockProvider
}

func hotplugTimeout(operation, volume string) error {
	return &opennebula.HotplugTimeoutError{
		Operation:            operation,
		Volume:               volume,
		Node:                 "node-1",
		Timeout:              3 * time.Minute,
		LastObservedAttached: false,
		LastObservedReady:    false,
		Cause:                errors.New("timed out waiting for attach"),
	}
}

func busyDiagnosis(classification, busyVolume string) opennebula.HotplugDiagnosis {
	return opennebula.HotplugDiagnosis{
		Node:           "node-1",
		LCMState:       "HOTPLUG",
		Classification: classification,
		Operation:      "attach",
		VolumeHandle:   busyVolume,
		DiskAttachFlag: "YES",
		AgeSeconds:     90,
	}
}

func TestHandleHotplugTimeoutDoesNotCountVMBusyWithAnotherDisk(t *testing.T) {
	cs, driver, mockProvider := newBusyTestServer(t, busyDiagnosis(opennebula.HotplugClassificationProgressing, "pvc-other"), 0)

	cs.handleHotplugTimeout(context.Background(), "node-1", "pvc-mine", nil, "attach", "disk", hotplugTimeout("attach", "pvc-mine"))

	_, recorded := driver.hotplugGuard.Snapshot()["node-1"]
	assert.False(t, recorded, "a VM busy hot-plugging another disk must not be recorded as a node failure")
	_, blocking := driver.hotplugGuard.Get("node-1")
	assert.False(t, blocking)
	mockProvider.AssertNotCalled(t, "NodeReady", mock.Anything, "node-1")
}

func TestHandleHotplugTimeoutCountsVMStuckOnAnotherDisk(t *testing.T) {
	cs, driver, mockProvider := newBusyTestServer(t, busyDiagnosis(opennebula.HotplugClassificationStuck, "pvc-other"), 1)

	cs.handleHotplugTimeout(context.Background(), "node-1", "pvc-mine", nil, "attach", "disk", hotplugTimeout("attach", "pvc-mine"))

	state, recorded := driver.hotplugGuard.Snapshot()["node-1"]
	require.True(t, recorded, "a VM stuck in HOTPLUG past the stuck threshold is still a node failure")
	assert.Equal(t, 1, state.FailureCount)
	assert.False(t, state.OpenNebulaReady)
	mockProvider.AssertExpectations(t)
}

func TestHandleHotplugTimeoutCountsOwnDiskHotplug(t *testing.T) {
	cs, driver, mockProvider := newBusyTestServer(t, busyDiagnosis(opennebula.HotplugClassificationProgressing, "pvc-mine"), 1)

	cs.handleHotplugTimeout(context.Background(), "node-1", "pvc-mine", nil, "attach", "disk", hotplugTimeout("attach", "pvc-mine"))

	state, recorded := driver.hotplugGuard.Snapshot()["node-1"]
	require.True(t, recorded, "a timeout on this volume's own hotplug keeps the existing behaviour")
	assert.Equal(t, 1, state.FailureCount)
	mockProvider.AssertExpectations(t)
}

func TestHotplugBusyWithOtherDisk(t *testing.T) {
	progressing := opennebula.HotplugClassificationProgressing
	cases := []struct {
		name      string
		diagnosis opennebula.HotplugDiagnosis
		want      bool
	}{
		{"other disk in flight", busyDiagnosis(progressing, "pvc-other"), true},
		{"own disk in flight", busyDiagnosis(progressing, "pvc-mine"), false},
		{"stuck on other disk", busyDiagnosis(opennebula.HotplugClassificationStuck, "pvc-other"), false},
		{"vm running", opennebula.HotplugDiagnosis{Classification: opennebula.HotplugClassificationReady}, false},
		{"no disk flagged in flight", func() opennebula.HotplugDiagnosis {
			d := busyDiagnosis(progressing, "pvc-other")
			d.DiskAttachFlag = ""
			return d
		}(), false},
		{"unknown busy disk", busyDiagnosis(progressing, ""), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, hotplugBusyWithOtherDisk(tc.diagnosis, "pvc-mine"))
		})
	}
}
