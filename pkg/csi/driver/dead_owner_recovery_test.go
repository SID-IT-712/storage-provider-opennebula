package driver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/opennebula"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	deadOwnerVolume  = "vol-dead"
	deadOwnerImageID = 488
	deadOwnerNewNode = "node-new"
	deadOwnerNewVMID = 181
	deadOwnerOldNode = "node-old"
	deadOwnerOldVMID = 160
)

// driftedMetadata is what InspectVolumeAttachment reports while the volume is
// still recorded on the old VM: the image lists it and its template has the disk.
func driftedMetadata(owners ...opennebula.OwnerVMRecord) *opennebula.VolumeAttachmentMetadata {
	metadata := &opennebula.VolumeAttachmentMetadata{
		VolumeHandle:    deadOwnerVolume,
		ImageID:         deadOwnerImageID,
		ImageState:      "USED_PERS",
		ImageRunningVMs: len(owners),
		RequestedNode:   deadOwnerNewNode,
		RequestedNodeID: deadOwnerNewVMID,
		OwnerVMs:        owners,
	}
	for _, owner := range owners {
		metadata.ImageVMIDs = append(metadata.ImageVMIDs, owner.VMID)
		if owner.Deleted || owner.VMState == "DONE" {
			continue
		}
		metadata.DiskRecords = append(metadata.DiskRecords, opennebula.VolumeDiskRecord{
			NodeName: owner.Name, NodeID: owner.VMID, DiskID: 12, Target: "sdd",
			VMState: owner.VMState, LCMState: owner.LCMState,
		})
	}
	return metadata
}

func releasedMetadata() *opennebula.VolumeAttachmentMetadata {
	return &opennebula.VolumeAttachmentMetadata{
		VolumeHandle:    deadOwnerVolume,
		ImageID:         deadOwnerImageID,
		ImageState:      "READY",
		RequestedNode:   deadOwnerNewNode,
		RequestedNodeID: deadOwnerNewVMID,
	}
}

func ownerRecord(vmID int, name, vmState, lcmState string) opennebula.OwnerVMRecord {
	return opennebula.OwnerVMRecord{VMID: vmID, Name: name, VMState: vmState, LCMState: lcmState}
}

func poweroffOwner() opennebula.OwnerVMRecord {
	return ownerRecord(deadOwnerOldVMID, deadOwnerOldNode, "POWEROFF", "LCM_INIT")
}

func unknownOwner() opennebula.OwnerVMRecord {
	return ownerRecord(deadOwnerOldVMID, deadOwnerOldNode, "ACTIVE", "UNKNOWN")
}

func outOfServiceNode(name string) *corev1.Node {
	node := newReadyNode(name, false)
	node.Spec.Taints = []corev1.Taint{{Key: outOfServiceTaintKey, Value: "nodeshutdown", Effect: corev1.TaintEffectNoExecute}}
	return node
}

func newDeadOwnerDriver(t *testing.T, objects ...runtime.Object) *Driver {
	t.Helper()
	pv, pvc := newLocalPVAndPVC(deadOwnerVolume, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pvc.Spec.VolumeName = pv.Name
	driver := newStickyTestDriver(t, append([]runtime.Object{pv, pvc}, objects...)...)
	driver.PluginConfig.OverrideVal(config.MetadataDriftQuarantineFailureThresholdVar, 2)
	driver.PluginConfig.OverrideVal(config.MetadataDriftQuarantineTTLSecondsVar, 1800)
	return driver
}

// expectPublishPreamble scripts the publish path up to the drift check: the
// volume and the new node exist and the volume is not on the new node yet.
func expectPublishPreamble(provider *MockOpenNebulaVolumeProviderTestify) {
	provider.On("VolumeExists", mock.Anything, deadOwnerVolume).Return(deadOwnerImageID, volumeSize, nil)
	provider.On("NodeExists", mock.Anything, deadOwnerNewNode).Return(deadOwnerNewVMID, nil)
	provider.On("GetVolumeInNode", mock.Anything, deadOwnerImageID, deadOwnerNewVMID).Return("", errors.New("not attached")).Twice()
}

func expectAttachSucceeds(provider *MockOpenNebulaVolumeProviderTestify) {
	provider.On("AttachVolume", mock.Anything, deadOwnerVolume, deadOwnerNewNode, false, mock.Anything).Return(nil).Once()
	provider.On("GetVolumeInNode", mock.Anything, deadOwnerImageID, deadOwnerNewVMID).Return("sdd", nil).Once()
}

func recoveredCount(driver *Driver) float64 {
	var metric dto.Metric
	if err := driver.metrics.metadataDriftTotal.WithLabelValues(metadataDriftClassification, metadataDriftOutcomeRecoveredDeadOwner).Write(&metric); err != nil {
		return -1
	}
	return metric.GetCounter().GetValue()
}

func assertRefusedAndQuarantined(t *testing.T, driver *Driver, err error, wantInMessage string) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "OpenNebula metadata drift")
	assert.Contains(t, err.Error(), wantInMessage)
	cm, cmErr := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(context.Background(), volumeQuarantineStateConfigMapName, metav1.GetOptions{})
	require.NoError(t, cmErr, "a refused publish still records its quarantine observation")
	var state VolumeQuarantineState
	require.NoError(t, json.Unmarshal([]byte(cm.Data[deadOwnerVolume]), &state))
	assert.Equal(t, 1, state.FailureCount)
	assert.Equal(t, metadataDriftReason, state.Reason)
	assert.Equal(t, float64(0), recoveredCount(driver))
}

func assertNoRecoveryAction(t *testing.T, provider *MockOpenNebulaVolumeProviderTestify) {
	t.Helper()
	provider.AssertNotCalled(t, "PoweroffVMHard", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "DetachVolumeFromStoppedVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestDeadOwnerPoweroffOwnerIsDetachedThenAttached(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(releasedMetadata(), nil).Once()
	expectAttachSucceeds(provider)

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	resp, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	require.NoError(t, err)
	assert.Equal(t, "sdd", resp.GetPublishContext()["volumeName"])
	assert.Equal(t, float64(1), recoveredCount(driver))
	provider.AssertNotCalled(t, "PoweroffVMHard", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertNotCalled(t, "NodeReady", mock.Anything, deadOwnerOldNode)
	provider.AssertExpectations(t)
}

func TestDeadOwnerUnknownOwnerWithOutOfServiceTaintIsPoweredOffDetachedThenAttached(t *testing.T) {
	driver := newDeadOwnerDriver(t, outOfServiceNode(deadOwnerOldNode))
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(unknownOwner()), nil).Once()
	poweroff := provider.On("PoweroffVMHard", mock.Anything, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once().NotBefore(poweroff)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(releasedMetadata(), nil).Once()
	expectAttachSucceeds(provider)

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	require.NoError(t, err)
	assert.Equal(t, float64(1), recoveredCount(driver))
	provider.AssertNotCalled(t, "NodeReady", mock.Anything, deadOwnerOldNode)
	provider.AssertExpectations(t)
}

func TestDeadOwnerRefusals(t *testing.T) {
	cases := []struct {
		name      string
		nodes     []runtime.Object
		owners    []opennebula.OwnerVMRecord
		wantInMsg string
	}{
		{
			name:      "UNKNOWN owner without the out-of-service taint",
			nodes:     []runtime.Object{newReadyNode(deadOwnerOldNode, false)},
			owners:    []opennebula.OwnerVMRecord{unknownOwner()},
			wantInMsg: "does not carry " + outOfServiceTaintKey,
		},
		{
			name:      "UNKNOWN owner whose Kubernetes node is missing",
			owners:    []opennebula.OwnerVMRecord{unknownOwner()},
			wantInMsg: "could not be checked",
		},
		{
			name:      "RUNNING owner",
			owners:    []opennebula.OwnerVMRecord{ownerRecord(deadOwnerOldVMID, deadOwnerOldNode, "ACTIVE", "RUNNING")},
			wantInMsg: "ACTIVE/RUNNING, not provably dead",
		},
		{
			name:      "RUNNING owner even with the out-of-service taint",
			nodes:     []runtime.Object{outOfServiceNode(deadOwnerOldNode)},
			owners:    []opennebula.OwnerVMRecord{ownerRecord(deadOwnerOldVMID, deadOwnerOldNode, "ACTIVE", "RUNNING")},
			wantInMsg: "not provably dead",
		},
		{
			name:      "owner state lookup error",
			owners:    []opennebula.OwnerVMRecord{{VMID: deadOwnerOldVMID, LookupError: "OpenNebula error [AUTHORIZATION]: denied"}},
			wantInMsg: "could not be read",
		},
		{
			name:      "owner in another state (SUSPENDED)",
			owners:    []opennebula.OwnerVMRecord{ownerRecord(deadOwnerOldVMID, deadOwnerOldNode, "SUSPENDED", "LCM_INIT")},
			wantInMsg: "not provably dead",
		},
		{
			name: "one dead owner beside one RUNNING owner",
			owners: []opennebula.OwnerVMRecord{
				poweroffOwner(),
				ownerRecord(170, "node-other", "ACTIVE", "RUNNING"),
			},
			wantInMsg: "owner VM 170 is ACTIVE/RUNNING",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			driver := newDeadOwnerDriver(t, tc.nodes...)
			provider := new(MockOpenNebulaVolumeProviderTestify)
			expectPublishPreamble(provider)
			provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(tc.owners...), nil).Once()

			cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
			_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
			assertRefusedAndQuarantined(t, driver, err, tc.wantInMsg)
			assertNoRecoveryAction(t, provider)
			provider.AssertExpectations(t)
		})
	}
}

func TestDeadOwnerMetadataWithoutOwnerStateIsRefused(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	metadata := driftedMetadata(poweroffOwner())
	metadata.OwnerVMs = nil
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(metadata, nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "has no state record")
	assertNoRecoveryAction(t, provider)
}

func TestDeadOwnerDoneOrDeletedOwnerProceedsWithoutDetach(t *testing.T) {
	for _, owner := range []opennebula.OwnerVMRecord{
		{VMID: deadOwnerOldVMID, Deleted: true},
		{VMID: deadOwnerOldVMID, Name: deadOwnerOldNode, VMState: "DONE", LCMState: "LCM_INIT"},
	} {
		t.Run(owner.VMState+map[bool]string{true: "deleted"}[owner.Deleted], func(t *testing.T) {
			driver := newDeadOwnerDriver(t)
			provider := new(MockOpenNebulaVolumeProviderTestify)
			expectPublishPreamble(provider)
			// The image still lists the gone VM after re-inspection: a DONE or
			// deleted VM holds no disk, so that record alone does not block.
			provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(owner), nil).Twice()
			expectAttachSucceeds(provider)

			cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
			_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
			require.NoError(t, err)
			assert.Equal(t, float64(1), recoveredCount(driver))
			provider.AssertNotCalled(t, "PoweroffVMHard", mock.Anything, mock.Anything, mock.Anything)
			provider.AssertNotCalled(t, "DetachVolumeFromStoppedVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			provider.AssertExpectations(t)
		})
	}
}

func TestDeadOwnerRefusedWhenReinspectionStillShowsAnOwner(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Twice()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "still holds volume")
	provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestDeadOwnerRefusedWhenDetachFails(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(errors.New("[one.vm.detach] locked")).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "detach from stopped owner VM 160 failed")
	provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestDeadOwnerRefusedWhenHardPoweroffFails(t *testing.T) {
	driver := newDeadOwnerDriver(t, outOfServiceNode(deadOwnerOldNode))
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(unknownOwner()), nil).Once()
	provider.On("PoweroffVMHard", mock.Anything, deadOwnerOldVMID, time.Duration(0)).Return(errors.New("timed out waiting for POWEROFF")).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "hard power-off of UNKNOWN owner VM 160 failed")
	provider.AssertNotCalled(t, "DetachVolumeFromStoppedVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestDeadOwnerKillSwitchOffKeepsTheOldRefusal(t *testing.T) {
	driver := newDeadOwnerDriver(t, outOfServiceNode(deadOwnerOldNode))
	driver.PluginConfig.OverrideVal(config.MetadataDriftDeadOwnerRecoveryVar, false)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "external OpenNebula repair is required")
	assert.NotContains(t, err.Error(), "dead-owner recovery", "with the switch off the message is the old one")
	assertNoRecoveryAction(t, provider)
	provider.AssertExpectations(t)
}

func TestDeadOwnerRecoveryClearsAnActiveQuarantine(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	_, active, err := driver.volumeQuarantine.MarkFailure(context.Background(), VolumeQuarantineState{
		VolumeID: deadOwnerVolume, Reason: metadataDriftReason, Classification: metadataDriftClassification,
		RequestedNode: deadOwnerNewNode, OwnerVMIDs: []int{deadOwnerOldVMID},
	}, 1, time.Hour)
	require.NoError(t, err)
	require.True(t, active)

	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(releasedMetadata(), nil).Once()
	expectAttachSucceeds(provider)

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err = cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	require.NoError(t, err)
	_, stillQuarantined := driver.volumeQuarantine.GetActive(deadOwnerVolume, time.Now().UTC())
	assert.False(t, stillQuarantined)
	provider.AssertExpectations(t)
}

func startStickyGrace(t *testing.T, driver *Driver) {
	t.Helper()
	require.NoError(t, driver.stickyAttachments.StartGrace(StickyAttachmentState{
		VolumeID:     deadOwnerVolume,
		NodeID:       deadOwnerOldNode,
		Backend:      "local",
		PVCNamespace: "default",
		PVCName:      "pvc-" + deadOwnerVolume,
		StartedAt:    time.Now().Add(-10 * time.Second),
		ExpiresAt:    time.Now().Add(90 * time.Second),
		GraceSeconds: 90,
		Reason:       "stateful_restart",
	}))
}

func expectOldNodeGone(provider *MockOpenNebulaVolumeProviderTestify) {
	provider.On("NodeExists", mock.Anything, deadOwnerOldNode).Return(-1, &opennebula.VMNotFoundError{Name: deadOwnerOldNode})
}

func TestStickyOldNodeGoneRecoversFromDeadOwner(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	startStickyGrace(t, driver)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectOldNodeGone(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(releasedMetadata(), nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	handled, resp, err := cs.reuseStickyAttachment(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode), deadOwnerImageID, deadOwnerNewVMID)
	require.NoError(t, err)
	assert.False(t, handled, "the publish goes on to attach on the new node")
	assert.Nil(t, resp)
	_, ok := driver.stickyAttachments.Get(deadOwnerVolume)
	assert.False(t, ok)
	assert.Equal(t, float64(1), recoveredCount(driver))
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestStickyOldNodeGoneWithNoOwnerClearsStaleState(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	startStickyGrace(t, driver)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectOldNodeGone(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(releasedMetadata(), nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	handled, _, err := cs.reuseStickyAttachment(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode), deadOwnerImageID, deadOwnerNewVMID)
	require.NoError(t, err)
	assert.False(t, handled)
	_, ok := driver.stickyAttachments.Get(deadOwnerVolume)
	assert.False(t, ok)
	assert.Equal(t, float64(0), recoveredCount(driver), "nothing was recovered, only stale state cleared")
	assertNoRecoveryAction(t, provider)
	provider.AssertNotCalled(t, "DetachVolume", mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestStickyOldNodeGoneWithLiveOwnerKeepsTheOldPath(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	startStickyGrace(t, driver)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectOldNodeGone(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(
		driftedMetadata(ownerRecord(deadOwnerOldVMID, "renamed-vm", "ACTIVE", "RUNNING")), nil).Once()
	// The old path's detach-drift check inspects against the old node.
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerOldNode).Return(
		driftedMetadata(ownerRecord(deadOwnerOldVMID, "renamed-vm", "ACTIVE", "RUNNING")), nil).Once()
	provider.On("DetachVolume", mock.Anything, deadOwnerVolume, deadOwnerOldNode).Return(errors.New("failed to check if node exists")).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	handled, _, err := cs.reuseStickyAttachment(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode), deadOwnerImageID, deadOwnerNewVMID)
	require.Error(t, err)
	assert.True(t, handled)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "the old detach-drift quarantine still applies")
	_, ok := driver.stickyAttachments.Get(deadOwnerVolume)
	assert.True(t, ok, "sticky state is kept while a live owner holds the volume")
	assertNoRecoveryAction(t, provider)
	provider.AssertExpectations(t)
}

func TestStickyOldNodeGoneKillSwitchOffKeepsTheOldPath(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	driver.PluginConfig.OverrideVal(config.MetadataDriftDeadOwnerRecoveryVar, false)
	startStickyGrace(t, driver)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectOldNodeGone(provider)
	provider.On("DetachVolume", mock.Anything, deadOwnerVolume, deadOwnerOldNode).Return(errors.New("failed to check if node exists")).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	handled, _, err := cs.reuseStickyAttachment(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode), deadOwnerImageID, deadOwnerNewVMID)
	require.Error(t, err)
	assert.True(t, handled)
	assert.Equal(t, codes.Internal, status.Code(err))
	provider.AssertNotCalled(t, "InspectVolumeAttachment", mock.Anything, mock.Anything, mock.Anything)
	assertNoRecoveryAction(t, provider)
	provider.AssertExpectations(t)
}

func TestDeadOwnerMixedDeadOwnersAreAllReleased(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	deleted := opennebula.OwnerVMRecord{VMID: 150, Deleted: true}
	done := opennebula.OwnerVMRecord{VMID: 155, Name: "node-done", VMState: "DONE", LCMState: "LCM_INIT"}
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(deleted, done, poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(deleted, done), nil).Once()
	expectAttachSucceeds(provider)

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	require.NoError(t, err)
	provider.AssertNumberOfCalls(t, "DetachVolumeFromStoppedVM", 1)
	provider.AssertExpectations(t)
}

func TestDeadOwnerRefusedWhenANewLiveOwnerAppearsAtReinspection(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()
	provider.On("DetachVolumeFromStoppedVM", mock.Anything, deadOwnerVolume, deadOwnerOldVMID, time.Duration(0)).Return(nil).Once()
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(
		driftedMetadata(ownerRecord(170, "node-other", "ACTIVE", "RUNNING")), nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
	assertRefusedAndQuarantined(t, driver, err, "owner VM 170 still holds volume")
	provider.AssertNotCalled(t, "AttachVolume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	provider.AssertExpectations(t)
}

func TestDeadOwnerBusyOwnerNodeDefersWithoutQuarantine(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	release := driver.operationLocks.Acquire(controllerNodeLockKey(deadOwnerOldNode))
	defer release()
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectPublishPreamble(provider)
	provider.On("InspectVolumeAttachment", mock.Anything, deadOwnerVolume, deadOwnerNewNode).Return(driftedMetadata(poweroffOwner()), nil).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	done := make(chan error, 1)
	go func() {
		_, err := cs.ControllerPublishVolume(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode))
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err))
		assert.Contains(t, err.Error(), "busy")
	case <-time.After(5 * time.Second):
		t.Fatal("publish blocked on the owner node lock instead of deferring")
	}
	_, err := driver.kubeRuntime.client.CoreV1().ConfigMaps("default").Get(context.Background(), volumeQuarantineStateConfigMapName, metav1.GetOptions{})
	assert.Error(t, err, "a busy owner node is not a drift observation")
	assertNoRecoveryAction(t, provider)
}

func TestStickyOldNodeGoneWithQuarantineDisabledKeepsTheOldPath(t *testing.T) {
	driver := newDeadOwnerDriver(t)
	driver.PluginConfig.OverrideVal(config.MetadataDriftQuarantineEnabledVar, false)
	startStickyGrace(t, driver)
	provider := new(MockOpenNebulaVolumeProviderTestify)
	expectOldNodeGone(provider)
	provider.On("DetachVolume", mock.Anything, deadOwnerVolume, deadOwnerOldNode).Return(errors.New("failed to check if node exists")).Once()

	cs := NewControllerServer(driver, provider, &MockSharedFilesystemProviderTestify{})
	handled, _, err := cs.reuseStickyAttachment(context.Background(), newPublishReq(deadOwnerVolume, deadOwnerNewNode), deadOwnerImageID, deadOwnerNewVMID)
	require.Error(t, err)
	assert.True(t, handled)
	provider.AssertNotCalled(t, "InspectVolumeAttachment", mock.Anything, mock.Anything, mock.Anything)
	assertNoRecoveryAction(t, provider)
}

func TestNodeHasTaint(t *testing.T) {
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(outOfServiceNode("tainted"), newReadyNode("clean", true)), enabled: true}
	ctx := context.Background()

	tainted, err := runtime.NodeHasTaint(ctx, "tainted", outOfServiceTaintKey)
	require.NoError(t, err)
	assert.True(t, tainted)

	tainted, err = runtime.NodeHasTaint(ctx, "clean", outOfServiceTaintKey)
	require.NoError(t, err)
	assert.False(t, tainted)

	_, err = runtime.NodeHasTaint(ctx, "missing", outOfServiceTaintKey)
	assert.Error(t, err, "a missing Node is an error, never 'no taint'")

	_, err = (&KubeRuntime{}).NodeHasTaint(ctx, "tainted", outOfServiceTaintKey)
	assert.Error(t, err)
}
