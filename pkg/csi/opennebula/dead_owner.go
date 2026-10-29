package opennebula

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
	"k8s.io/klog/v2"
)

const defaultDeadOwnerWaitTimeout = 2 * time.Minute

// PoweroffVMHard powers a VM off without asking its guest, then waits until
// OpenNebula reports POWEROFF. It is the step that turns an UNKNOWN owner (a
// host the monitor lost) into a VM whose disks OpenNebula will detach.
func (p *PersistentDiskVolumeProvider) PoweroffVMHard(ctx context.Context, vmID int, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultDeadOwnerWaitTimeout
	}
	vmController := p.ctrl.VM(vmID)
	vmInfo, err := vmController.InfoContext(ctx, false)
	if err != nil {
		return fmt.Errorf("failed to read VM %d before hard power-off: %w", vmID, err)
	}
	state, lcmState, err := vmInfo.State()
	if err != nil {
		return fmt.Errorf("failed to read state of VM %d before hard power-off: %w", vmID, err)
	}
	if state == vm.Poweroff {
		return nil
	}
	// @why: the caller classified the VM before taking its lock; a host that
	//   came back meanwhile must not have its live VM powered off.
	if state != vm.Active || lcmState != vm.Unknown {
		return fmt.Errorf("refusing to hard power off VM %d in state %s/%s: only an UNKNOWN VM is powered off here", vmID, state.String(), lcmState.String())
	}
	klog.V(0).InfoS("Hard powering off dead owner VM to release a local RWO volume", "vmID", vmID, "vmName", vmInfo.Name)
	if err := vmController.PoweroffHardContext(ctx); err != nil {
		return fmt.Errorf("failed to hard power off VM %d: %w", vmID, err)
	}
	return p.waitForVMPoweroff(ctx, vmID, timeout)
}

func (p *PersistentDiskVolumeProvider) waitForVMPoweroff(ctx context.Context, vmID int, timeout time.Duration) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(p.pollInterval())
	defer ticker.Stop()

	lastState := "unknown"
	for {
		vmInfo, err := p.ctrl.VM(vmID).InfoContext(deadlineCtx, false)
		if err == nil {
			state, lcmState, stateErr := vmInfo.State()
			if stateErr == nil && state == vm.Poweroff {
				return nil
			}
			if stateErr == nil {
				lastState = fmt.Sprintf("%s/%s", state.String(), lcmState.String())
				if poweroffFailed(state, lcmState) {
					return fmt.Errorf("hard power-off of VM %d did not take: VM is %s", vmID, lastState)
				}
			}
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("timed out after %s waiting for VM %d to reach POWEROFF (last state %s)", timeout, vmID, lastState)
		case <-ticker.C:
		}
	}
}

// DetachVolumeFromStoppedVM detaches a volume from a POWEROFF VM. It differs
// from DetachVolume in two ways that both matter for a dead owner: it addresses
// the VM by ID (its Kubernetes node may be gone), and it waits only for the disk
// to leave the VM template, never for the VM to be RUNNING.
func (p *PersistentDiskVolumeProvider) DetachVolumeFromStoppedVM(ctx context.Context, volume string, vmID int, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultDeadOwnerWaitTimeout
	}
	volumeID, _, err := p.VolumeExists(ctx, volume)
	if err != nil {
		return fmt.Errorf("failed to check if volume exists: %w", err)
	}
	if volumeID == -1 {
		return fmt.Errorf("volume %s was not found", volume)
	}

	vmController := p.ctrl.VM(vmID)
	vmInfo, err := vmController.InfoContext(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to read VM %d before detach: %w", vmID, err)
	}
	state, lcmState, err := vmInfo.State()
	if err != nil {
		return fmt.Errorf("failed to read state of VM %d before detach: %w", vmID, err)
	}
	if state != vm.Poweroff {
		return fmt.Errorf("refusing to detach volume %s from VM %d in state %s/%s: only a POWEROFF VM is detached here", volume, vmID, state.String(), lcmState.String())
	}

	diskID, found := diskIDForImage(vmInfo, volumeID)
	if !found {
		klog.V(1).InfoS("Volume already absent from stopped VM", "volume", volume, "vmID", vmID)
		return nil
	}
	klog.V(0).InfoS("Detaching local RWO volume from stopped owner VM", "volume", volume, "vmID", vmID, "diskID", diskID)
	if err := vmController.Disk(diskID).DetachContext(ctx); err != nil {
		return fmt.Errorf("failed to detach volume %s from stopped VM %d: %w", volume, vmID, err)
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(p.pollInterval())
	defer ticker.Stop()
	for {
		current, infoErr := vmController.InfoContext(deadlineCtx, true)
		if infoErr == nil {
			if _, stillAttached := diskIDForImage(current, volumeID); !stillAttached {
				return nil
			}
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("timed out after %s waiting for volume %s to leave stopped VM %d", timeout, volume, vmID)
		case <-ticker.C:
		}
	}
}

// poweroffFailed reports a state a VM reaches when poweroff-hard did not take:
// running again, or any *_FAILURE LCM state. UNKNOWN and SHUTDOWN_POWEROFF are
// still "not yet", since the action is in flight from UNKNOWN.
func poweroffFailed(state vm.State, lcmState vm.LCMState) bool {
	if state != vm.Active {
		return false
	}
	return lcmState == vm.Running || strings.HasSuffix(lcmState.String(), "FAILURE")
}

func diskIDForImage(vmInfo *vm.VM, imageID int) (int, bool) {
	if vmInfo == nil {
		return 0, false
	}
	for _, disk := range vmInfo.Template.GetDisks() {
		diskImageID, err := disk.GetI(shared.ImageID)
		if err != nil || diskImageID != imageID {
			continue
		}
		diskID, err := disk.GetI(shared.DiskID)
		if err != nil {
			continue
		}
		return diskID, true
	}
	return 0, false
}

func (p *PersistentDiskVolumeProvider) pollInterval() time.Duration {
	if p.hotplugPollWait > 0 {
		return p.hotplugPollWait
	}
	return time.Second
}
