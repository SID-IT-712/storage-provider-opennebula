package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/opennebula"
	"k8s.io/klog/v2"
)

const (
	outOfServiceTaintKey                   = "node.kubernetes.io/out-of-service"
	metadataDriftOutcomeRecoveredDeadOwner = "recovered_dead_owner"
)

type deadOwnerAction string

const (
	// deadOwnerGone: the VM is DONE or deleted, so it holds nothing to detach.
	deadOwnerGone deadOwnerAction = "gone"
	// deadOwnerDetach: the VM is POWEROFF, so OpenNebula detaches its disk.
	deadOwnerDetach deadOwnerAction = "detach"
	// deadOwnerPoweroffDetach: the VM is UNKNOWN and its node is out of service.
	deadOwnerPoweroffDetach deadOwnerAction = "poweroff_hard_detach"
)

type deadOwnerStep struct {
	owner  opennebula.OwnerVMRecord
	action deadOwnerAction
}

// resolveDeadOwners releases a local RWO volume from owner VMs that are
// provably dead, so a publish to another node can proceed. It returns true only
// when every conflicting owner was dead and a fresh inspection shows no live
// owner left. Any doubt (a RUNNING owner, an unreadable state, an UNKNOWN owner
// whose node is not out of service, a failed step) returns false, and the caller
// refuses the publish exactly as before. heldNodeLocks names node locks the
// caller already holds, which are not taken again.
func (s *ControllerServer) resolveDeadOwners(ctx context.Context, volumeID, requestedNode string, metadata *opennebula.VolumeAttachmentMetadata, heldNodeLocks ...string) (bool, error) {
	if !s.deadOwnerRecoveryEnabled() || metadata == nil {
		return false, nil
	}
	recoverer, ok := s.volumeProvider.(opennebula.DeadOwnerRecoverer)
	if !ok {
		return false, nil
	}
	inspector, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector)
	if !ok {
		return false, nil
	}

	// @why: every owner is classified before any one is touched, so a live owner
	//   among dead ones leaves the dead ones as they were, not half-recovered.
	steps, err := s.planDeadOwnerRecovery(ctx, metadata)
	if err != nil || len(steps) == 0 {
		return false, err
	}

	release, err := s.tryLockOwnerNodes(steps, append([]string{requestedNode}, heldNodeLocks...))
	if err != nil {
		return false, err
	}
	defer release()

	for _, step := range steps {
		switch step.action {
		case deadOwnerPoweroffDetach:
			if err := recoverer.PoweroffVMHard(ctx, step.owner.VMID, 0); err != nil {
				return false, fmt.Errorf("hard power-off of UNKNOWN owner VM %d failed: %w", step.owner.VMID, err)
			}
			fallthrough
		case deadOwnerDetach:
			// @gotcha: never DetachVolume here; it polls nodeReady, which needs the
			//   VM RUNNING, so on a stopped VM it could only time out.
			if err := recoverer.DetachVolumeFromStoppedVM(ctx, volumeID, step.owner.VMID, 0); err != nil {
				return false, fmt.Errorf("detach from stopped owner VM %d failed: %w", step.owner.VMID, err)
			}
		case deadOwnerGone:
		}
		klog.V(0).InfoS("Released local RWO volume from dead owner VM",
			"volumeID", volumeID, "requestedNode", requestedNode, "ownerVMID", step.owner.VMID,
			"ownerName", step.owner.Name, "vmState", step.owner.VMState, "lcmState", step.owner.LCMState, "action", step.action)
	}

	after, err := inspector.InspectVolumeAttachment(ctx, volumeID, requestedNode)
	if err != nil {
		return false, fmt.Errorf("re-inspection after dead-owner recovery failed: %w", err)
	}
	if after == nil {
		return false, fmt.Errorf("re-inspection after dead-owner recovery returned no metadata")
	}
	residual := after.ConflictingOwnerVMIDs()
	for _, ownerID := range residual {
		owner, found := after.OwnerVM(ownerID)
		if !found || !ownerGone(owner) {
			return false, fmt.Errorf("owner VM %d still holds volume %s after dead-owner recovery", ownerID, volumeID)
		}
	}
	if len(residual) == 0 && attachmentMetadataDriftDetected(after) {
		return false, fmt.Errorf("image of volume %s still reports %d running VM(s) with no known owner after dead-owner recovery", volumeID, after.ImageRunningVMs)
	}
	return true, nil
}

// errDeadOwnerNodeBusy means an owner VM's node lock is held by another
// operation. The publish is retried later; it is not a drift observation.
var errDeadOwnerNodeBusy = errors.New("an owner VM's node is busy with another operation")

// tryLockOwnerNodes takes the node lock of every owner that will be touched.
// @gotcha: TryAcquire, never Acquire. The caller already holds the volume lock,
//
//	and other paths take node then volume, so blocking here can deadlock both.
func (s *ControllerServer) tryLockOwnerNodes(steps []deadOwnerStep, heldNodeLocks []string) (func(), error) {
	held := map[string]struct{}{}
	for _, name := range heldNodeLocks {
		held[controllerNodeLockKey(name)] = struct{}{}
	}
	releases := []func(){}
	releaseAll := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for _, step := range steps {
		if step.action == deadOwnerGone || strings.TrimSpace(step.owner.Name) == "" {
			continue
		}
		key := controllerNodeLockKey(step.owner.Name)
		if _, ok := held[key]; ok {
			continue
		}
		release, ok := s.driver.operationLocks.TryAcquire(key)
		if !ok {
			releaseAll()
			return nil, fmt.Errorf("%w: node %s (owner VM %d)", errDeadOwnerNodeBusy, step.owner.Name, step.owner.VMID)
		}
		held[key] = struct{}{}
		releases = append(releases, release)
	}
	return releaseAll, nil
}

func (s *ControllerServer) planDeadOwnerRecovery(ctx context.Context, metadata *opennebula.VolumeAttachmentMetadata) ([]deadOwnerStep, error) {
	ownerIDs := metadata.ConflictingOwnerVMIDs()
	steps := make([]deadOwnerStep, 0, len(ownerIDs))
	for _, ownerID := range ownerIDs {
		owner, found := metadata.OwnerVM(ownerID)
		if !found {
			return nil, fmt.Errorf("owner VM %d has no state record", ownerID)
		}
		action, err := s.classifyDeadOwner(ctx, owner)
		if err != nil {
			return nil, err
		}
		steps = append(steps, deadOwnerStep{owner: owner, action: action})
	}
	return steps, nil
}

func (s *ControllerServer) classifyDeadOwner(ctx context.Context, owner opennebula.OwnerVMRecord) (deadOwnerAction, error) {
	if owner.LookupError != "" {
		return "", fmt.Errorf("state of owner VM %d could not be read: %s", owner.VMID, owner.LookupError)
	}
	if ownerGone(owner) {
		return deadOwnerGone, nil
	}
	if owner.VMState == "POWEROFF" {
		return deadOwnerDetach, nil
	}
	if owner.VMState == "ACTIVE" && owner.LCMState == "UNKNOWN" {
		tainted, err := s.nodeOutOfService(ctx, owner.Name)
		if err != nil {
			return "", fmt.Errorf("owner VM %d is UNKNOWN and its node %q could not be checked for %s: %w", owner.VMID, owner.Name, outOfServiceTaintKey, err)
		}
		if !tainted {
			return "", fmt.Errorf("owner VM %d is UNKNOWN and its node %q does not carry %s", owner.VMID, owner.Name, outOfServiceTaintKey)
		}
		return deadOwnerPoweroffDetach, nil
	}
	return "", fmt.Errorf("owner VM %d is %s/%s, not provably dead", owner.VMID, owner.VMState, owner.LCMState)
}

func ownerGone(owner opennebula.OwnerVMRecord) bool {
	return owner.LookupError == "" && (owner.Deleted || owner.VMState == "DONE")
}

func (s *ControllerServer) nodeOutOfService(ctx context.Context, nodeName string) (bool, error) {
	if s == nil || s.driver == nil || s.driver.kubeRuntime == nil {
		return false, fmt.Errorf("kubernetes runtime is not configured")
	}
	// Kubernetes node names are lowercase DNS names; an OpenNebula VM name may not be.
	return s.driver.kubeRuntime.NodeHasTaint(ctx, strings.ToLower(strings.TrimSpace(nodeName)), outOfServiceTaintKey)
}

func (s *ControllerServer) deadOwnerRecoveryEnabled() bool {
	if s == nil || s.driver == nil {
		return false
	}
	enabled, ok := s.driver.PluginConfig.GetBool(config.MetadataDriftDeadOwnerRecoveryVar)
	return !ok || enabled
}

func (s *ControllerServer) recordDeadOwnerRecovered(ctx context.Context, runtimeCtx *VolumeRuntimeContext, volumeID, requestedNode string, metadata *opennebula.VolumeAttachmentMetadata) {
	s.driver.metrics.RecordMetadataDrift(metadataDriftClassification, metadataDriftOutcomeRecoveredDeadOwner)
	owners := []string{}
	if metadata != nil {
		for _, owner := range metadata.OwnerVMs {
			state := vmStateLabel(owner.VMState, owner.LCMState)
			if owner.Deleted {
				state = "DELETED"
			}
			owners = append(owners, fmt.Sprintf("%d(%s)", owner.VMID, state))
		}
	}
	s.recordPVCEventFromRuntimeContext(ctx, runtimeCtx, eventReasonOpenNebulaDeadOwnerRecovered,
		fmt.Sprintf("released local RWO volume %s from dead owner VM(s) %s before publishing to node %s", volumeID, strings.Join(owners, ","), requestedNode))
}

// vmStateLabel renders an OpenNebula state pair as POWEROFF or ACTIVE/UNKNOWN;
// the LCM state is only meaningful for ACTIVE VMs.
func vmStateLabel(vmState, lcmState string) string {
	if lcmState == "" || lcmState == "LCM_INIT" {
		return vmState
	}
	return vmState + "/" + lcmState
}
