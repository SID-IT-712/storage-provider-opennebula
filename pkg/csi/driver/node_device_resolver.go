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
	"syscall"
	"time"

	"github.com/OpenNebula/storage-provider-opennebula/pkg/csi/config"
	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

var (
	nodeByIDGlob     = filepath.Glob
	nodeEvalSymlinks = filepath.EvalSymlinks
	nodeWriteFile    = os.WriteFile
	nodeReadDir      = os.ReadDir
	nodeSysfsRoot    = "/sys"
	// nodeBlockDeviceNumber returns a block device's device number (Rdev); tests replace it.
	nodeBlockDeviceNumber = blockDeviceNumber
	deviceResolverNow     = func() time.Time { return time.Now() }
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
	if serial != "" {
		return r.resolveSerial(ctx, volumeID, imageID, serial, started, timeout)
	}
	if volumeID != "" {
		if path, ok := r.resolveFromCache(volumeID, ""); ok {
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "cache", Latency: deviceResolverNow().Sub(started)}, nil
		}
	}
	if devicePath, result, err := r.resolveAlias(volumeName, started); err == nil {
		r.remember(volumeID, imageID, "", located{DevicePath: devicePath})
		return devicePath, result, nil
	}

	deadline := deviceResolverNow().Add(timeout)
	for {
		if timeout > 0 && deviceResolverNow().After(deadline) {
			break
		}
		r.runRecoverySequence(ctx)
		if devicePath, result, err := r.resolveAlias(volumeName, started); err == nil {
			r.remember(volumeID, imageID, "", located{DevicePath: devicePath})
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
	return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s waiting for device path for volume %q (checked %s)", timeout, volumeName, strings.Join(candidates, ", "))
}

// resolveSerial resolves a request that carries a device serial. Every result
// carries the proof that the device is the one with that serial.
// @gotcha: a request with a serial never resolves by alias. volumeName is OpenNebula's
// disk TARGET, not the kernel name: /dev/<TARGET> can be another volume's disk.
func (r *NodeDeviceResolver) resolveSerial(ctx context.Context, volumeID, imageID, serial string, started time.Time, timeout time.Duration) (string, deviceResolutionResult, error) {
	if volumeID != "" {
		if path, ok := r.resolveFromCache(volumeID, serial); ok {
			return path.DevicePath, deviceResolutionResult{ResolvedBy: "cache", Proof: path.Proof, Latency: deviceResolverNow().Sub(started)}, nil
		}
	}
	found, err := locateBySerial(r.exec, serial)
	if err == nil {
		r.remember(volumeID, imageID, serial, found)
		return found.DevicePath, deviceResolutionResult{ResolvedBy: found.resolvedBy(""), Proof: found.Proof, Latency: deviceResolverNow().Sub(started)}, nil
	}
	if !errors.Is(err, errSerialNotFound) {
		return "", deviceResolutionResult{}, err
	}

	deadline := deviceResolverNow().Add(timeout)
	for timeout <= 0 || !deviceResolverNow().After(deadline) {
		r.runRecoverySequence(ctx)
		found, err = locateBySerial(r.exec, serial)
		if err == nil {
			r.remember(volumeID, imageID, serial, found)
			return found.DevicePath, deviceResolutionResult{ResolvedBy: found.resolvedBy("-recovery"), Proof: found.Proof, Latency: deviceResolverNow().Sub(started)}, nil
		}
		if !errors.Is(err, errSerialNotFound) {
			return "", deviceResolutionResult{}, err
		}
		if timeout <= 0 {
			break
		}
		nodeDeviceSleep(defaultNodeDevicePollPeriod)
	}
	return "", deviceResolutionResult{}, fmt.Errorf("timed out after %s: %w", timeout, serialNotFoundError(serial))
}

func (r *NodeDeviceResolver) Invalidate(volumeID string) {
	if r == nil || strings.TrimSpace(volumeID) == "" {
		return
	}
	r.mu.Lock()
	delete(r.cache, volumeID)
	r.mu.Unlock()
}

// resolveFromCache returns a cached device. For a serial request the hit is
// re-proven: the kernel/udev/lsblk serial must match, or, when no source knows
// it, locateBySerial must still find the same device.
func (r *NodeDeviceResolver) resolveFromCache(volumeID, serial string) (located, bool) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" {
		return located{}, false
	}
	r.mu.RLock()
	entry, ok := r.cache[volumeID]
	r.mu.RUnlock()
	if !ok {
		return located{}, false
	}
	if r.cacheTTL > 0 && deviceResolverNow().Sub(entry.LastConfirmedAt) > r.cacheTTL {
		r.Invalidate(volumeID)
		return located{}, false
	}
	if serial != "" && entry.Serial != "" && !strings.EqualFold(strings.TrimSpace(serial), strings.TrimSpace(entry.Serial)) {
		r.Invalidate(volumeID)
		return located{}, false
	}
	if entry.DevicePath == "" {
		r.Invalidate(volumeID)
		return located{}, false
	}
	if _, err := nodeVolumePathStat(entry.DevicePath); err != nil {
		r.Invalidate(volumeID)
		return located{}, false
	}
	hit := located{DevicePath: entry.DevicePath, ByIDPath: entry.ByIDPath}
	if serial == "" {
		return hit, true
	}
	switch identifyDevice(r.exec, entry.DevicePath, serial) {
	case serialMatch:
		hit.Proof = proofKernel
		return hit, true
	case serialUnknown:
		if found, err := locateBySerial(r.exec, serial); err == nil && sameBlockDevice(found.DevicePath, entry.DevicePath) {
			return found, true
		}
	}
	r.Invalidate(volumeID)
	return located{}, false
}

// deviceProof says how a device was proven to carry a requested serial.
type deviceProof string

const (
	// proofKernel: an identity source (the kernel's sysfs serial, else udev or
	// lsblk) reports exactly the requested serial for this device.
	proofKernel deviceProof = "kernel"
	// proofByID: an exact /dev/disk/by-id name for the serial resolves to the
	// device, and no identity source contradicts it.
	proofByID deviceProof = "by-id"
)

// located is a device found for a serial, with the proof that it is the one.
type located struct {
	DevicePath string
	ByIDPath   string
	Proof      deviceProof
}

func (l located) resolvedBy(suffix string) string {
	if l.Proof == proofKernel {
		return "kernel-serial" + suffix
	}
	return "by-id" + suffix
}

var errSerialNotFound = errors.New("serial not found")

func serialNotFoundError(serial string) error {
	return fmt.Errorf("%w: no block device under %s carries serial %s, and no %s entry names it",
		errSerialNotFound, filepath.Join(nodeSysfsRoot, "block"), serial, filepath.Join(defaultDiskPath, "disk", "by-id"))
}

// locateBySerial is the only way a request with a serial finds its device.
//  1. The kernel: the one whole disk under <sysfs>/block whose serial is exactly
//     serial. It needs no udev, so it works before (or without) by-id links.
//     More than one such disk is an error; it never picks one.
//  2. Else an exact /dev/disk/by-id name, accepted only if no identity source
//     contradicts it (covers disks with no readable serial).
//
// It never uses the volume's TARGET alias.
func locateBySerial(exec utilexec.Interface, serial string) (located, error) {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return located{}, fmt.Errorf("serial is required")
	}
	byID := byIDEntriesForSerial(serial)
	matches := []string{}
	if entries, err := nodeReadDir(filepath.Join(nodeSysfsRoot, "block")); err == nil {
		for _, entry := range entries {
			name := entry.Name()
			if !isCandidateWholeDisk(name) {
				continue
			}
			if kernelSerial, _ := kernelBlockSerial(name); kernelSerial != "" && strings.EqualFold(kernelSerial, serial) {
				matches = append(matches, name)
			}
		}
	}
	if len(matches) > 1 {
		return located{}, fmt.Errorf("serial %s is ambiguous: block devices %s all carry it; refusing to choose", serial, strings.Join(matches, ", "))
	}
	if len(matches) == 1 {
		devicePath := filepath.Join(defaultDiskPath, matches[0])
		if _, err := nodeVolumePathStat(devicePath); err == nil {
			found := located{DevicePath: devicePath, Proof: proofKernel}
			for _, link := range byID {
				if sameBlockDevice(link.resolved, devicePath) {
					found.ByIDPath = link.path
					break
				}
			}
			return found, nil
		}
	}

	var found *located
	for _, link := range byID {
		if identifyDevice(exec, link.resolved, serial) == serialMismatch {
			klog.V(2).InfoS("Skipping by-id entry whose device reports another serial",
				"byID", link.path, "device", link.resolved, "serial", serial)
			continue
		}
		if found != nil && !sameBlockDevice(found.DevicePath, link.resolved) {
			return located{}, fmt.Errorf("serial %s is ambiguous: by-id entries resolve to %s and %s; refusing to choose", serial, found.DevicePath, link.resolved)
		}
		if found == nil {
			found = &located{DevicePath: link.resolved, ByIDPath: link.path, Proof: proofByID}
		}
	}
	if found != nil {
		return *found, nil
	}
	return located{}, serialNotFoundError(serial)
}

type byIDEntry struct {
	path     string
	resolved string
}

// byIDEntriesForSerial lists the existing by-id entries whose name names serial.
func byIDEntriesForSerial(serial string) []byIDEntry {
	candidates, err := nodeByIDGlob(filepath.Join(defaultDiskPath, "disk", "by-id", "*"))
	if err != nil {
		return nil
	}
	out := []byIDEntry{}
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
		out = append(out, byIDEntry{path: candidate, resolved: resolved})
	}
	return out
}

// isCandidateWholeDisk filters <sysfs>/block names: no loop, ram/zram, optical
// or device-mapper devices, and no partitions.
func isCandidateWholeDisk(name string) bool {
	for _, prefix := range []string{"loop", "ram", "zram", "sr", "dm-"} {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	if _, err := os.Stat(filepath.Join(nodeSysfsRoot, "block", name, "partition")); err == nil {
		return false
	}
	return true
}

// sameBlockDevice reports whether two paths are the same block device: both
// must be block devices with the same device number.
func sameBlockDevice(a, b string) bool {
	na, okA := nodeBlockDeviceNumber(a)
	nb, okB := nodeBlockDeviceNumber(b)
	return okA && okB && na == nb
}

func blockDeviceNumber(path string) (uint64, bool) {
	info, err := os.Stat(path)
	if err != nil || info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		return 0, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Rdev), true //nolint:unconvert // Rdev is uint32 on some platforms
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
	hostPaths, err := nodeByIDGlob(filepath.Join(nodeSysfsRoot, "class", "scsi_host", "host*", "scan"))
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

func (r *NodeDeviceResolver) remember(volumeID, imageID, serial string, found located) {
	if r == nil || !r.cacheEnabled || strings.TrimSpace(volumeID) == "" || strings.TrimSpace(found.DevicePath) == "" {
		return
	}
	r.mu.Lock()
	r.cache[volumeID] = nodeDeviceCacheEntry{
		VolumeID:        volumeID,
		ImageID:         imageID,
		Serial:          serial,
		DevicePath:      found.DevicePath,
		ByIDPath:        found.ByIDPath,
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

// identifyDevice is the only evidence function for "does this device carry
// serial": match, mismatch, or unknown. Only serialMatch proves identity;
// unknown never does (fail closed). The kernel's sysfs serial decides alone
// when known. Otherwise udev ID_SERIAL_SHORT / ID_SCSI_SERIAL and lsblk SERIAL
// (stdout only) are compared exactly; any known value that differs is a mismatch.
// @gotcha: the node plugin container has no udev db, so udevadm/lsblk usually know nothing
// there; the kernel serial is the primary proof. Never reintroduce substring matching.
// @gotcha: never compare udev ID_SERIAL: it is model-prefixed (0QEMU_QEMU_HARDDISK_onecsi-11).
func identifyDevice(exec utilexec.Interface, devicePath, serial string) serialEvidence {
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
	if output, err := exec.Command("udevadm", "info", "--query=property", "--name", devicePath).Output(); err == nil {
		for _, line := range strings.Split(string(output), "\n") {
			key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
			if !ok || (key != "ID_SERIAL_SHORT" && key != "ID_SCSI_SERIAL") {
				continue
			}
			if value = strings.TrimSpace(value); value == "" {
				continue
			}
			known = true
			if strings.EqualFold(value, serial) {
				return serialMatch
			}
		}
	}
	if output, err := exec.Command("lsblk", "-ndo", "SERIAL", devicePath).Output(); err == nil {
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
// SCSI unit serial number VPD page (0x80), else the virtio-blk serial, else the
// NVMe controller serial. It
// returns "" with no error when the kernel exposes neither, or when the path
// is not a whole disk (a partition has no <sysfs>/block/<name>).
func kernelDeviceSerial(devicePath string) (string, error) {
	resolved, err := nodeEvalSymlinks(devicePath)
	if err != nil {
		return "", err
	}
	return kernelBlockSerial(filepath.Base(resolved))
}

// kernelBlockSerial reads the serial of the whole disk <sysfs>/block/<name>.
func kernelBlockSerial(name string) (string, error) {
	blockDir := filepath.Join(nodeSysfsRoot, "block", name)
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
	// virtio-blk exposes <block>/serial, NVMe <block>/device/serial.
	for _, file := range []string{filepath.Join(blockDir, "serial"), filepath.Join(blockDir, "device", "serial")} {
		if raw, err := os.ReadFile(file); err == nil {
			if serial := trimDeviceSerial(string(raw)); serial != "" {
				return serial, nil
			}
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
