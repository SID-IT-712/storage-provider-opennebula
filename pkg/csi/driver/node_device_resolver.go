package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

var (
	nodeByIDGlob      = filepath.Glob
	nodeEvalSymlinks  = filepath.EvalSymlinks
	nodeWriteFile     = os.WriteFile
	nodeReadDir       = os.ReadDir
	nodeSysfsRoot     = "/sys"
	deviceResolverNow = func() time.Time { return time.Now() }
)

const (
	publishContextDeviceSerial      = "deviceSerial"
	publishContextOpenNebulaImageID = "opennebulaImageID"
)

type nodeDeviceCacheEntry struct {
	VolumeID        string
	ImageID         string
	Serial          string
	DevicePath      string
	ByIDPath        string
	LastConfirmedAt time.Time
}

type NodeDeviceResolver struct {
	exec               utilexec.Interface
	cacheEnabled       bool
	cacheTTL           time.Duration
	udevSettleTimeout  time.Duration
	rescanOnMiss       bool
	deviceCandidatesFn func(volumeName string) []string
	mu                 sync.RWMutex
	cache              map[string]nodeDeviceCacheEntry
	udevadmMissingLog  bool
}

func NewNodeDeviceResolver(cfg config.CSIPluginConfig, exec utilexec.Interface, candidatesFn func(volumeName string) []string) *NodeDeviceResolver {
	cacheEnabled, ok := cfg.GetBool(config.NodeDeviceCacheEnabledVar)
	if !ok {
		cacheEnabled = true
	}
	cacheTTLSeconds, ok := cfg.GetInt(config.NodeDeviceCacheTTLSecondsVar)
	if !ok || cacheTTLSeconds <= 0 {
		cacheTTLSeconds = 600
	}
	udevSettleTimeoutSeconds, ok := cfg.GetInt(config.NodeDeviceUdevSettleTimeoutSecondsVar)
	if !ok {
		udevSettleTimeoutSeconds = 10
	}
	rescanOnMiss, ok := cfg.GetBool(config.NodeDeviceRescanOnMissEnabledVar)
	if !ok {
		rescanOnMiss = true
	}
	return &NodeDeviceResolver{
		exec:               exec,
		cacheEnabled:       cacheEnabled,
		cacheTTL:           time.Duration(cacheTTLSeconds) * time.Second,
		udevSettleTimeout:  time.Duration(udevSettleTimeoutSeconds) * time.Second,
		rescanOnMiss:       rescanOnMiss,
		deviceCandidatesFn: candidatesFn,
		cache:              map[string]nodeDeviceCacheEntry{},
	}
}

func (r *NodeDeviceResolver) Resolve(ctx context.Context, volumeID, volumeName string, publishContext map[string]string, timeout time.Duration) (string, deviceResolutionResult, error) {
	started := deviceResolverNow()
	serial := strings.TrimSpace(publishContext[publishContextDeviceSerial])
	imageID := strings.TrimSpace(publishContext[publishContextOpenNebulaImageID])
	if volumeID != "" {
		if path, ok := r.resolveFromCache(volumeID, serial); ok {
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "cache", Latency: deviceResolverNow().Sub(started)}, nil
		}
	}

	// @gotcha: a request with a serial never resolves by alias. volumeName is OpenNebula's
	//   disk TARGET, not the kernel name: /dev/<TARGET> can be another volume's disk.
	if serial != "" {
		if path, ok := r.resolveByID(serial); ok {
			r.remember(volumeID, imageID, serial, path.DevicePath, path.ByIDPath)
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "by-id", Latency: deviceResolverNow().Sub(started)}, nil
		}
	} else if devicePath, result, err := r.resolveAlias(volumeName, started); err == nil {
		r.remember(volumeID, imageID, serial, devicePath, "")
		return devicePath, result, nil
	}

	deadline := deviceResolverNow().Add(timeout)
	for {
		if timeout > 0 && deviceResolverNow().After(deadline) {
			break
		}
		r.runRecoverySequence(ctx)
		if serial != "" {
			if path, ok := r.resolveByID(serial); ok {
				r.remember(volumeID, imageID, serial, path.DevicePath, path.ByIDPath)
				return path.DevicePath, deviceResolutionResult{ResolvedBy: "by-id-recovery", Latency: deviceResolverNow().Sub(started)}, nil
			}
		} else if devicePath, result, err := r.resolveAlias(volumeName, started); err == nil {
			r.remember(volumeID, imageID, serial, devicePath, "")
			if result.ResolvedBy == "exact" {
				result.ResolvedBy = "exact-recovery"
			} else {
				result.ResolvedBy = "alias-recovery"
			}
			return devicePath, result, nil
		}
		if timeout <= 0 {
			break
		}
		nodeDeviceSleep(defaultNodeDevicePollPeriod)
	}

	candidates := []string{}
	if r.deviceCandidatesFn != nil {
		candidates = r.deviceCandidatesFn(volumeName)
	}
	if serial != "" {
		return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s waiting for device path for volume %q (serial %s, checked %s)", timeout, volumeName, serial, strings.Join(candidates, ", "))
	}
	return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s waiting for device path for volume %q (checked %s)", timeout, volumeName, strings.Join(candidates, ", "))
}

func (r *NodeDeviceResolver) Invalidate(volumeID string) {
	if r == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	r.mu.Lock()
	delete(r.cache, volumeID)
	r.mu.Unlock()
}

type resolvedDevicePath struct {
	DevicePath string
	ByIDPath   string
}

func (r *NodeDeviceResolver) resolveFromCache(volumeID, serial string) (resolvedDevicePath, bool) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" {
		return resolvedDevicePath{}, false
	}
	r.mu.RLock()
	entry, ok := r.cache[volumeID]
	r.mu.RUnlock()
	if !ok {
		return resolvedDevicePath{}, false
	}
	if r.cacheTTL > 0 && deviceResolverNow().Sub(entry.LastConfirmedAt) > r.cacheTTL {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if serial != "" && entry.Serial != "" && !strings.EqualFold(strings.TrimSpace(serial), strings.TrimSpace(entry.Serial)) {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if entry.DevicePath == "" {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if _, err := nodeVolumePathStat(entry.DevicePath); err != nil {
		r.Invalidate(volumeID)
		return resolvedDevicePath{}, false
	}
	if serial != "" {
		if !deviceMatchesSerial(r.exec, entry.DevicePath, serial) {
			r.Invalidate(volumeID)
			return resolvedDevicePath{}, false
		}
	}
	return resolvedDevicePath{DevicePath: entry.DevicePath, ByIDPath: entry.ByIDPath}, true
}

func (r *NodeDeviceResolver) resolveByID(serial string) (resolvedDevicePath, bool) {
	if strings.TrimSpace(serial) == "" {
		return resolvedDevicePath{}, false
	}
	candidates, err := nodeByIDGlob(filepath.Join(defaultDiskPath, "disk", "by-id", "*"))
	if err != nil {
		return resolvedDevicePath{}, false
	}
	for _, candidate := range candidates {
		if !byIDNameHasSerial(filepath.Base(candidate), serial) {
			continue
		}
		resolved, err := nodeEvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if _, err := nodeVolumePathStat(resolved); err != nil {
			continue
		}
		// An exact by-id name is accepted when no source knows the device's
		// serial; a source that knows it and disagrees vetoes the candidate.
		if deviceSerialEvidenceFor(r.exec, resolved, serial) == serialMismatch {
			klog.V(2).InfoS("Skipping by-id candidate whose device reports another serial",
				"candidate", candidate, "device", resolved, "serial", serial)
			continue
		}
		return resolvedDevicePath{DevicePath: resolved, ByIDPath: candidate}, true
	}
	return resolvedDevicePath{}, false
}

// byIDNameHasSerial reports whether a /dev/disk/by-id entry name names the
// serial as a whole trailing token: the name equals the serial or ends with
// "_"+serial or "-"+serial (case-insensitive).
// @gotcha: never a substring match - onecsi-43 is a substring of onecsi-439.
func byIDNameHasSerial(base, serial string) bool {
	base = strings.ToLower(strings.TrimSpace(base))
	serial = strings.ToLower(strings.TrimSpace(serial))
	if serial == "" || base == "" {
		return false
	}
	return base == serial || strings.HasSuffix(base, "_"+serial) || strings.HasSuffix(base, "-"+serial)
}

func (r *NodeDeviceResolver) resolveAlias(volumeName string, started time.Time) (string, deviceResolutionResult, error) {
	candidates := []string{}
	if r.deviceCandidatesFn != nil {
		candidates = r.deviceCandidatesFn(volumeName)
	}
	expected := pathJoin(defaultDiskPath, filepath.Base(volumeName))
	var lastErr error
	for _, candidate := range candidates {
		if _, err := nodeVolumePathStat(candidate); err == nil {
			result := deviceResolutionResult{
				ResolvedBy: "exact",
				Latency:    deviceResolverNow().Sub(started),
			}
			if candidate != expected {
				result.ResolvedBy = "alias"
			}
			return candidate, result, nil
		} else if !os.IsNotExist(err) {
			lastErr = err
		}
	}
	if lastErr != nil {
		return "", deviceResolutionResult{}, lastErr
	}
	return "", deviceResolutionResult{}, fmt.Errorf("device not found")
}

func (r *NodeDeviceResolver) runRecoverySequence(ctx context.Context) {
	if r == nil || r.exec == nil {
		return
	}
	if r.udevSettleTimeout > 0 {
		timeoutSeconds := int(r.udevSettleTimeout / time.Second)
		if timeoutSeconds < 1 {
			timeoutSeconds = 1
		}
		if output, err := r.exec.CommandContext(ctx, "udevadm", "settle", fmt.Sprintf("--timeout=%d", timeoutSeconds)).CombinedOutput(); err != nil {
			if errors.Is(err, osexec.ErrNotFound) || strings.Contains(err.Error(), "executable file not found") {
				if r.markUdevadmMissingLogged() {
					klog.V(2).InfoS("udevadm is unavailable; device recovery will continue with SCSI rescan only", "err", err)
				}
			} else {
				klog.V(4).InfoS("udevadm settle failed during device recovery", "err", err, "output", strings.TrimSpace(string(output)))
			}
		}
	}
	if !r.rescanOnMiss {
		return
	}
	hostPaths, err := nodeByIDGlob("/sys/class/scsi_host/host*/scan")
	if err != nil {
		return
	}
	for _, hostPath := range hostPaths {
		if err := nodeWriteFile(hostPath, []byte("- - -"), 0o200); err != nil {
			klog.V(5).InfoS("SCSI rescan write failed", "path", hostPath, "err", err)
		}
	}
}

func (r *NodeDeviceResolver) markUdevadmMissingLogged() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.udevadmMissingLog {
		return false
	}
	r.udevadmMissingLog = true
	return true
}

func (r *NodeDeviceResolver) remember(volumeID, imageID, serial, devicePath, byIDPath string) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" || strings.TrimSpace(devicePath) == "" {
		return
	}
	r.mu.Lock()
	r.cache[volumeID] = nodeDeviceCacheEntry{
		VolumeID:        volumeID,
		ImageID:         imageID,
		Serial:          serial,
		DevicePath:      devicePath,
		ByIDPath:        byIDPath,
		LastConfirmedAt: deviceResolverNow().UTC(),
	}
	r.mu.Unlock()
}

type serialEvidence int

const (
	serialUnknown serialEvidence = iota
	serialMatch
	serialMismatch
)

// deviceMatchesSerial reports whether the block device at devicePath carries
// serial. It fails closed: when no source knows the device's serial it is
// false. Sources, in order: the kernel (sysfs), udevadm, lsblk; every compare
// is exact (case-insensitive).
// @gotcha: the node plugin container has no udev db, so udevadm/lsblk report no serial
// there; the kernel sysfs serial is the primary proof. Never reintroduce substring matching.
func deviceMatchesSerial(exec utilexec.Interface, devicePath, serial string) bool {
	if strings.TrimSpace(serial) == "" {
		return true
	}
	return deviceSerialEvidenceFor(exec, devicePath, serial) == serialMatch
}

// deviceSerialEvidenceFor says whether the device affirmatively carries
// serial, affirmatively carries another one, or whether no source knows.
func deviceSerialEvidenceFor(exec utilexec.Interface, devicePath, serial string) serialEvidence {
	serial = strings.TrimSpace(serial)
	kernelSerial, err := kernelDeviceSerial(devicePath)
	if err != nil {
		klog.V(4).InfoS("Kernel device serial unavailable", "device", devicePath, "err", err)
	}
	if kernelSerial != "" {
		if strings.EqualFold(kernelSerial, serial) {
			return serialMatch
		}
		return serialMismatch
	}
	if exec == nil {
		return serialUnknown
	}
	known := false
	if output, err := exec.Command("udevadm", "info", "--query=property", "--name", devicePath).CombinedOutput(); err == nil {
		for _, line := range strings.Split(string(output), "\n") {
			if !strings.HasPrefix(line, "ID_SERIAL=") && !strings.HasPrefix(line, "ID_SERIAL_SHORT=") {
				continue
			}
			value := strings.TrimSpace(strings.SplitN(line, "=", 2)[1])
			if value == "" {
				continue
			}
			known = true
			if strings.EqualFold(value, serial) {
				return serialMatch
			}
		}
	}
	if output, err := exec.Command("lsblk", "-ndo", "SERIAL", devicePath).CombinedOutput(); err == nil {
		if value := strings.TrimSpace(string(output)); value != "" {
			known = true
			if strings.EqualFold(value, serial) {
				return serialMatch
			}
		}
	}
	if known {
		return serialMismatch
	}
	return serialUnknown
}

// kernelDeviceSerial reads a whole disk's serial from the kernel's sysfs: the
// SCSI unit serial number VPD page (0x80), else the virtio-blk serial. It
// returns "" with no error when the kernel exposes neither, or when the path
// is not a whole disk (a partition has no <sysfs>/block/<name>).
func kernelDeviceSerial(devicePath string) (string, error) {
	resolved, err := nodeEvalSymlinks(devicePath)
	if err != nil {
		return "", err
	}
	blockDir := filepath.Join(nodeSysfsRoot, "block", filepath.Base(resolved))
	if _, err := os.Stat(blockDir); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if raw, err := os.ReadFile(filepath.Join(blockDir, "device", "vpd_pg80")); err == nil {
		if serial := parseVPDPage80(raw); serial != "" {
			return serial, nil
		}
	}
	if raw, err := os.ReadFile(filepath.Join(blockDir, "serial")); err == nil {
		if serial := trimDeviceSerial(string(raw)); serial != "" {
			return serial, nil
		}
	}
	return "", nil
}

// parseVPDPage80 extracts the serial from an SPC Unit Serial Number VPD page:
// byte 1 is the page code (0x80), bytes 2-3 the big-endian page length, and the
// serial follows from byte 4. A malformed page yields "".
func parseVPDPage80(raw []byte) string {
	if len(raw) < 4 || raw[1] != 0x80 {
		return ""
	}
	length := int(raw[2])<<8 | int(raw[3])
	if 4+length > len(raw) {
		return ""
	}
	return trimDeviceSerial(string(raw[4 : 4+length]))
}

func trimDeviceSerial(value string) string {
	return strings.Trim(value, " \t\r\n\x00")
}

func pathJoin(parts ...string) string {
	return filepath.Join(parts...)
}
