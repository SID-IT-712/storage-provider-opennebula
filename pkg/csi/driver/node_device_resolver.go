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

// NodeDeviceResolver finds a volume's block device by the serial OpenNebula gave
// its disk (onecsi-<imageID>), and only by that serial.
type NodeDeviceResolver struct {
	exec              utilexec.Interface
	udevSettleTimeout time.Duration
	rescanOnMiss      bool
	mu                sync.Mutex
	udevadmMissingLog bool
}

func NewNodeDeviceResolver(cfg config.CSIPluginConfig, exec utilexec.Interface) *NodeDeviceResolver {
	udevSettleTimeoutSeconds, ok := cfg.GetInt(config.NodeDeviceUdevSettleTimeoutSecondsVar)
	if !ok {
		udevSettleTimeoutSeconds = 10
	}
	rescanOnMiss, ok := cfg.GetBool(config.NodeDeviceRescanOnMissEnabledVar)
	if !ok {
		rescanOnMiss = true
	}
	return &NodeDeviceResolver{
		exec:              exec,
		udevSettleTimeout: time.Duration(udevSettleTimeoutSeconds) * time.Second,
		rescanOnMiss:      rescanOnMiss,
	}
}

// Resolve returns the device that provably carries the publish context's
// deviceSerial. A context without one is refused (errNoDeviceSerial): it is
// never resolved by volumeName, the disk's OpenNebula TARGET.
// @gotcha: TARGET is not the guest's kernel name (the context CD-ROM shifts it,
// and hot-plug churn drifts it further), so /dev/<TARGET> or its vd/xvd alias can
// be another volume's disk, possibly mounted. Guessing it once staged one
// volume's filesystem for another. Nothing is cached: every call is proven again.
func (r *NodeDeviceResolver) Resolve(ctx context.Context, volumeID, volumeName string, publishContext map[string]string, timeout time.Duration) (string, deviceResolutionResult, error) {
	started := deviceResolverNow()
	serial := strings.TrimSpace(publishContext[publishContextDeviceSerial])
	if serial == "" {
		return "", deviceResolutionResult{}, noDeviceSerialError(volumeID, volumeName)
	}
	return r.resolveSerial(ctx, volumeName, serial, started, timeout)
}

// noDeviceSerialError is the refusal for a publish context with no deviceSerial.
func noDeviceSerialError(volumeID, volumeName string) error {
	return fmt.Errorf("%w (volume %q, TARGET %q); re-attach the volume so the controller publishes its serial",
		errNoDeviceSerial, volumeID, volumeName)
}

// resolveSerial resolves a request that carries a device serial, through
// locateBySerial only. Every result carries the proof. Not-found, not-yet and
// ambiguous are retried until the timeout (a stale twin usually disappears).
// @gotcha: a request with a serial never resolves by alias. volumeName is OpenNebula's
// disk TARGET, not the kernel name: /dev/<TARGET> can be another volume's disk.
func (r *NodeDeviceResolver) resolveSerial(ctx context.Context, volumeName, serial string, started time.Time, timeout time.Duration) (string, deviceResolutionResult, error) {
	found, err := locateBySerial(r.exec, serial)
	if err == nil {
		return found.DevicePath, deviceResolutionResult{ResolvedBy: found.resolvedBy(""), Proof: found.Proof, Latency: deviceResolverNow().Sub(started)}, nil
	}
	if !isRetryableLocateError(err) {
		return "", deviceResolutionResult{}, err
	}

	deadline := deviceResolverNow().Add(timeout)
	for timeout <= 0 || !deviceResolverNow().After(deadline) {
		r.runRecoverySequence(ctx)
		found, err = locateBySerial(r.exec, serial)
		if err == nil {
			return found.DevicePath, deviceResolutionResult{ResolvedBy: found.resolvedBy("-recovery"), Proof: found.Proof, Latency: deviceResolverNow().Sub(started)}, nil
		}
		if !isRetryableLocateError(err) {
			return "", deviceResolutionResult{}, err
		}
		if timeout <= 0 {
			break
		}
		nodeDeviceSleep(defaultNodeDevicePollPeriod)
	}
	return "", deviceResolutionResult{}, &locateError{
		kind: kindOf(err),
		msg:  fmt.Sprintf("after %s: %v (TARGET %s is not used for serial requests)", timeout, err, volumeName),
	}
}

// deviceProof says how a device was proven to carry a requested serial.
type deviceProof string

const (
	// proofKernel: the one whole disk whose kernel sysfs serial is exactly the
	// requested serial.
	proofKernel deviceProof = "kernel"
	// proofByID: no disk reports the serial in sysfs, an exact /dev/disk/by-id
	// name for it resolves to the device, and no identity source contradicts it.
	proofByID deviceProof = "by-id"
)

// located is a device found for a serial, with the proof that it is the one.
// ByIDPath is set only for proofByID.
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

var (
	// errSerialNotFound: nothing carries or names the serial yet. Retryable.
	errSerialNotFound = errors.New("serial not found")
	// errSerialNotYet: the one disk with the serial has no device node yet. Retryable.
	errSerialNotYet = errors.New("serial device node not present yet")
	// errSerialAmbiguous: more than one device carries the serial. Retryable;
	// at the deadline it is a refusal (FailedPrecondition), never a pick.
	errSerialAmbiguous = errors.New("serial ambiguous")
	// errNoDeviceResolver: nothing can prove a serial without the resolver.
	errNoDeviceResolver = errors.New("no device resolver")
	// errNoDeviceSerial: the publish context names no serial, so nothing can be
	// proven. A refusal (FailedPrecondition), never a guess by target name.
	errNoDeviceSerial = errors.New("publish context has no deviceSerial; refusing to resolve by target name")
)

// locateError carries a readable message and a sentinel kind for errors.Is.
type locateError struct {
	kind error
	msg  string
}

func (e *locateError) Error() string { return e.msg }
func (e *locateError) Unwrap() error { return e.kind }

func kindOf(err error) error {
	for _, kind := range []error{errSerialAmbiguous, errSerialNotYet, errSerialNotFound} {
		if errors.Is(err, kind) {
			return kind
		}
	}
	return err
}

func isRetryableLocateError(err error) bool {
	return errors.Is(err, errSerialNotFound) || errors.Is(err, errSerialNotYet) || errors.Is(err, errSerialAmbiguous)
}

// locateBySerial is the only way a request with a serial finds its device.
//  1. The kernel: scan <sysfs>/block for whole disks whose kernel serial is
//     exactly serial. Exactly one → that disk, or a not-yet error while its
//     device node is missing. More than one → ambiguous. It needs no udev.
//  2. Only when NO disk reports the serial: an exact /dev/disk/by-id name,
//     accepted only if no identity source contradicts it (covers disks with no
//     readable serial).
//
// It never uses the volume's TARGET alias.
// @gotcha: a kernel match never falls through to by-id: a stale by-id link would
// then name another disk while the real one is still appearing.
func locateBySerial(exec utilexec.Interface, serial string) (located, error) {
	serial = strings.TrimSpace(serial)
	if serial == "" {
		return located{}, fmt.Errorf("serial is required")
	}
	matches := kernelSerialMatches(serial)
	switch {
	case len(matches) > 1:
		return located{}, &locateError{kind: errSerialAmbiguous, msg: fmt.Sprintf("ambiguous: %s carry serial %s; refusing to choose", strings.Join(matches, ", "), serial)}
	case len(matches) == 1:
		devicePath := filepath.Join(defaultDiskPath, matches[0])
		if _, err := nodeVolumePathStat(devicePath); err != nil {
			return located{}, &locateError{kind: errSerialNotYet, msg: fmt.Sprintf("block device %s carries serial %s but %s is not present yet: %v", matches[0], serial, devicePath, err)}
		}
		return located{DevicePath: devicePath, Proof: proofKernel}, nil
	}

	var found *located
	for _, link := range byIDEntriesForSerial(serial) {
		if identifyDevice(exec, link.resolved, serial) == serialMismatch {
			klog.V(2).InfoS("Skipping by-id entry whose device reports another serial",
				"byID", link.path, "device", link.resolved, "serial", serial)
			continue
		}
		if found != nil && found.DevicePath != link.resolved && !sameBlockDevice(found.DevicePath, link.resolved) {
			return located{}, &locateError{kind: errSerialAmbiguous, msg: fmt.Sprintf("ambiguous: by-id entries for serial %s resolve to %s and %s; refusing to choose", serial, found.DevicePath, link.resolved)}
		}
		if found == nil {
			found = &located{DevicePath: link.resolved, ByIDPath: link.path, Proof: proofByID}
		}
	}
	if found != nil {
		return *found, nil
	}
	return located{}, &locateError{kind: errSerialNotFound, msg: fmt.Sprintf("no block device under %s carries serial %s, and no %s entry names it",
		filepath.Join(nodeSysfsRoot, "block"), serial, filepath.Join(defaultDiskPath, "disk", "by-id"))}
}

// kernelSerialMatches lists the whole disks under <sysfs>/block whose kernel
// serial is exactly serial. Blank serials never match.
func kernelSerialMatches(serial string) []string {
	entries, err := nodeReadDir(filepath.Join(nodeSysfsRoot, "block"))
	if err != nil {
		return nil
	}
	matches := []string{}
	for _, entry := range entries {
		name := entry.Name()
		if !isCandidateWholeDisk(name) || !blockDeviceRunning(name) {
			continue
		}
		if kernelSerial, _ := kernelBlockSerial(name); kernelSerial != "" && strings.EqualFold(kernelSerial, serial) {
			matches = append(matches, name)
		}
	}
	return matches
}

// blockDeviceRunning is false for a SCSI device whose device/state exists and
// is not "running" (offline, deleted): a detached disk can linger there.
func blockDeviceRunning(name string) bool {
	raw, err := os.ReadFile(filepath.Join(nodeSysfsRoot, "block", name, "device", "state"))
	if err != nil {
		return true
	}
	return strings.TrimSpace(string(raw)) == "running"
}

// byIDPathFor returns a by-id entry naming serial that is the same block device
// as devicePath, for display; "" when there is none.
func byIDPathFor(devicePath, serial string) string {
	for _, link := range byIDEntriesForSerial(serial) {
		if link.resolved == devicePath || sameBlockDevice(link.resolved, devicePath) {
			return link.path
		}
	}
	return ""
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

// isCandidateWholeDisk skips loop, ram/zram, optical and device-mapper devices.
// Partitions need no check: /sys/block lists whole disks only (a partition is a
// subdirectory of its disk there).
func isCandidateWholeDisk(name string) bool {
	for _, prefix := range []string{"loop", "ram", "zram", "sr", "dm-"} {
		if strings.HasPrefix(name, prefix) {
			return false
		}
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

// byIDNameHasSerial reports whether a /dev/disk/by-id entry name is exactly a
// name udev gives the disk with this serial: the serial itself,
// "<bus/model>_"+serial (scsi-0QEMU_QEMU_HARDDISK_<serial>, nvme-..._<serial>),
// or "virtio-"+serial (virtio-blk). Case-insensitive.
// @gotcha: never a substring match (onecsi-43 is in onecsi-439), and never a bare
// "-"+serial suffix: serials contain '-', so "x-onecsi-43" would match.
func byIDNameHasSerial(base, serial string) bool {
	base = strings.ToLower(strings.TrimSpace(base))
	serial = strings.ToLower(strings.TrimSpace(serial))
	if serial == "" || base == "" {
		return false
	}
	return base == serial || base == "virtio-"+serial || strings.HasSuffix(base, "_"+serial)
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
