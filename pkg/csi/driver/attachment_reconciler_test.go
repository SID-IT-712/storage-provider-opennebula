package driver

import (
	"context"
	"testing"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func newAttachmentTestDriver(objects ...runtime.Object) *Driver {
	cfg := config.LoadConfiguration()
	cfg.OverrideVal(config.StuckAttachmentReconcilerEnabledVar, true)
	runtime := &KubeRuntime{client: fake.NewSimpleClientset(objects...), enabled: true}
	return &Driver{
		name:           DefaultDriverName,
		PluginConfig:   cfg,
		kubeRuntime:    runtime,
		metrics:        NewDriverMetrics("test", "test"),
		operationLocks: NewOperationLocks(),
		hotplugGuard:   NewHotplugGuard(time.Minute),
	}
}

func TestAttachmentReconcilerDetachesOrphanAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	driver := newAttachmentTestDriver(pv, pvc)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{{
		VolumeHandle: "vol-1",
		ImageID:      1,
		NodeName:     "node-a",
		NodeID:       101,
		Backend:      "local",
	}}, nil).Once()
	mockProvider.On("GetVolumeInNode", mock.Anything, 1, 101).Return("vdb", nil).Once()
	mockProvider.On("DetachVolume", mock.Anything, "vol-1", "node-a").Return(nil).Once()
	mockProvider.On("ResolveVolumeSizeBytes", mock.Anything, "vol-1").Return(int64(1024), nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.orphanSeen["vol-1@node-a"] = time.Now().Add(-2 * reconciler.orphanGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))
	mockProvider.AssertExpectations(t)
}

func TestAttachmentReconcilerDeletesStaleVolumeAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-1", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-1"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: DefaultDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
	driver := newAttachmentTestDriver(pv, pvc, va)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.staleVASeen["va-1"] = time.Now().Add(-2 * reconciler.staleVAGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().Get(context.Background(), "va-1", metav1.GetOptions{})
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

// foreignDriverName is any CSI driver that is not ours. Longhorn is the one
// that was actually damaged on 2026-09-15.
const foreignDriverName = "driver.longhorn.io"

// TestAttachmentReconcilerLeavesForeignDriverVolumeAttachment is the regression
// test for the incident: the reconciler deleted 1,214 VolumeAttachments
// belonging to another CSI driver because it only checked `pv.Spec.CSI != nil`.
// Another driver's volume handle is never among OpenNebula's observed
// attachments, so every one of them looked stale. Deleting them detached live
// volumes from running pods.
func TestAttachmentReconcilerLeavesForeignDriverVolumeAttachment(t *testing.T) {
	pv, pvc := newLocalPVAndPVC("vol-foreign", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Spec.CSI.Driver = foreignDriverName

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-foreign"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: foreignDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}

	driver := newAttachmentTestDriver(pv, pvc, va)
	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	// OpenNebula observes no attachments of its own — precisely the state in
	// which the unfixed reconciler classified every foreign attachment as stale.
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	// Push it past the grace period, so surviving cannot be mistaken for "not
	// due yet" — without the driver filter this VA would be deleted right here.
	reconciler.staleVASeen["va-foreign"] = time.Now().Add(-2 * reconciler.staleVAGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().
		Get(context.Background(), "va-foreign", metav1.GetOptions{})
	assert.NoError(t, err, "another CSI driver's VolumeAttachment must survive reconciliation")
	mockProvider.AssertExpectations(t)
}

// TestAttachmentReconcilerHonoursCustomDriverName guards the other half of the
// fix. The driver's name is configurable (DriverOptions.DriverName), so
// filtering against the DefaultDriverName constant instead of the instance's
// own name would make a custom-named deployment skip its OWN volumes and
// silently reconcile nothing — a regression that reports no errors at all.
func TestAttachmentReconcilerHonoursCustomDriverName(t *testing.T) {
	const customDriverName = "csi.opennebula.example.com"

	pv, pvc := newLocalPVAndPVC("vol-custom", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
	pv.Spec.CSI.Driver = customDriverName

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-custom"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: customDriverName,
			NodeName: "node-a",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv.Name,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}

	driver := newAttachmentTestDriver(pv, pvc, va)
	driver.name = customDriverName

	mockProvider := &MockOpenNebulaVolumeProviderTestify{}
	mockProvider.On("ListCurrentAttachments", mock.Anything).Return([]opennebula.ObservedAttachment{}, nil).Once()

	server := NewControllerServer(driver, mockProvider, &MockSharedFilesystemProviderTestify{})
	reconciler := NewAttachmentReconciler(server)
	reconciler.staleVASeen["va-custom"] = time.Now().Add(-2 * reconciler.staleVAGrace)

	require.NoError(t, reconciler.ReconcileOnce(context.Background()))

	_, err := driver.kubeRuntime.client.StorageV1().VolumeAttachments().
		Get(context.Background(), "va-custom", metav1.GetOptions{})
	assert.Error(t, err, "the driver's own stale VolumeAttachment must still be cleaned up under a custom driver name")
	mockProvider.AssertExpectations(t)
}
