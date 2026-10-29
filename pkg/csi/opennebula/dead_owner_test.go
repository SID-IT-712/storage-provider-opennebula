package opennebula

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	oneNoExistsCode = 0x0400
	oneActionCode   = 0x0800
	vmStatePoweroff = 8
	vmStateDone     = 6
	vmStateActive   = 3
	lcmRunning      = 3
	lcmUnknown      = 16
)

type fakeDeadVM struct {
	name     string
	state    int
	lcm      int
	hasDisk  bool
	infoErr  string // a non-NO_EXISTS failure of one.vm.info
	stuckOff bool   // poweroff-hard is accepted but the VM never reaches POWEROFF
	revives  bool   // poweroff-hard is accepted and the VM comes back RUNNING
}

// fakeDeadOwnerRPC serves one image and a set of VMs. A VM missing from vms
// answers NO_EXISTS; a DONE VM is left out of the pool listing, as OpenNebula's
// default state filter (-1) does.
type fakeDeadOwnerRPC struct {
	mu        sync.Mutex
	imageID   int
	imageName string
	imageVMs  []int
	vms       map[int]*fakeDeadVM
	calls     []string
}

func (f *fakeDeadOwnerRPC) vmXML(id int, v *fakeDeadVM) string {
	disk := ""
	if v.hasDisk {
		disk = fmt.Sprintf("<DISK><DISK_ID>3</DISK_ID><IMAGE_ID>%d</IMAGE_ID><TARGET>sdd</TARGET></DISK>", f.imageID)
	}
	return fmt.Sprintf("<VM><ID>%d</ID><NAME>%s</NAME><STATE>%d</STATE><LCM_STATE>%d</LCM_STATE><TEMPLATE>%s</TEMPLATE></VM>", id, v.name, v.state, v.lcm, disk)
}

func (f *fakeDeadOwnerRPC) handle(call xmlRPCCall) (bool, string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	params := make([]string, 0, len(call.Params))
	for _, p := range call.Params {
		params = append(params, xmlRPCParam(p.Value.Inner))
	}
	f.calls = append(f.calls, call.MethodName+" "+strings.Join(params[1:], ","))

	switch call.MethodName {
	case "one.imagepool.info", "one.image.info":
		vms := ""
		for _, id := range f.imageVMs {
			vms += fmt.Sprintf("<ID>%d</ID>", id)
		}
		image := fmt.Sprintf("<IMAGE><ID>%d</ID><NAME>%s</NAME><SIZE>1024</SIZE><STATE>8</STATE><RUNNING_VMS>%d</RUNNING_VMS><VMS>%s</VMS><TEMPLATE></TEMPLATE></IMAGE>",
			f.imageID, f.imageName, len(f.imageVMs), vms)
		if call.MethodName == "one.image.info" {
			return true, image, 0
		}
		return true, "<IMAGE_POOL>" + image + "</IMAGE_POOL>", 0
	case "one.vmpool.info":
		ids := make([]int, 0, len(f.vms))
		for id := range f.vms {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		var pool strings.Builder
		for _, id := range ids {
			if f.vms[id].state == vmStateDone {
				continue
			}
			pool.WriteString(f.vmXML(id, f.vms[id]))
		}
		return true, "<VM_POOL>" + pool.String() + "</VM_POOL>", 0
	case "one.vm.info":
		id, _ := strconv.Atoi(params[1])
		v, ok := f.vms[id]
		if !ok {
			return false, fmt.Sprintf("[one.vm.info] Error getting virtual machine [%d].", id), oneNoExistsCode
		}
		if v.infoErr != "" {
			return false, v.infoErr, 0x0200
		}
		return true, f.vmXML(id, v), 0
	case "one.vm.action":
		id, _ := strconv.Atoi(params[2])
		v := f.vms[id]
		if params[1] != "poweroff-hard" || v == nil {
			return false, "unexpected action " + params[1], oneActionCode
		}
		switch {
		case v.revives:
			v.state, v.lcm = vmStateActive, lcmRunning
		case !v.stuckOff:
			v.state, v.lcm = vmStatePoweroff, 0
		}
		return true, strconv.Itoa(id), 0
	case "one.vm.detach":
		id, _ := strconv.Atoi(params[1])
		v := f.vms[id]
		if v == nil || v.state != vmStatePoweroff {
			return false, "[one.vm.detach] Wrong state to perform action", oneActionCode
		}
		v.hasDisk = false
		return true, strconv.Itoa(id), 0
	}
	return false, "unexpected method " + call.MethodName, 0x1000
}

func (f *fakeDeadOwnerRPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serveFakeXMLRPCWithCode(w, r, f.handle)
}

func (f *fakeDeadOwnerRPC) calledPrefix(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func newDeadOwnerFake(vms map[int]*fakeDeadVM, imageVMs ...int) *fakeDeadOwnerRPC {
	if vms == nil {
		vms = map[int]*fakeDeadVM{}
	}
	vms[181] = &fakeDeadVM{name: "node-new", state: vmStateActive, lcm: lcmRunning}
	return &fakeDeadOwnerRPC{imageID: 7, imageName: "pvc-dead", imageVMs: imageVMs, vms: vms}
}

func TestInspectVolumeAttachmentRecordsOwnerVMStates(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{
		160: {name: "node-off", state: vmStatePoweroff, hasDisk: true},
		161: {name: "node-lost", state: vmStateActive, lcm: lcmUnknown, hasDisk: true},
		162: {name: "node-done", state: vmStateDone},
		164: {name: "node-forbidden", state: vmStateDone, infoErr: "[one.vm.info] User not authorized"},
	}, 160, 161, 162, 163, 164)
	provider := newFakeOneProvider(t, fake)

	metadata, err := provider.InspectVolumeAttachment(context.Background(), "pvc-dead", "node-new")
	require.NoError(t, err)
	assert.Equal(t, []int{160, 161, 162, 163, 164}, metadata.ConflictingOwnerVMIDs())

	byID := map[int]OwnerVMRecord{}
	for _, owner := range metadata.OwnerVMs {
		byID[owner.VMID] = owner
	}
	assert.Equal(t, OwnerVMRecord{VMID: 160, Name: "node-off", VMState: "POWEROFF", LCMState: "LCM_INIT"}, byID[160])
	assert.Equal(t, OwnerVMRecord{VMID: 161, Name: "node-lost", VMState: "ACTIVE", LCMState: "UNKNOWN"}, byID[161])
	assert.Equal(t, OwnerVMRecord{VMID: 162, Name: "node-done", VMState: "DONE", LCMState: "LCM_INIT"}, byID[162], "a DONE VM is read by ID, since the pool leaves it out")
	assert.Equal(t, OwnerVMRecord{VMID: 163, Deleted: true}, byID[163], "a VM in the image's list that answers NO_EXISTS was deleted")
	assert.False(t, byID[164].Deleted, "only NO_EXISTS proves deletion")
	assert.Contains(t, byID[164].LookupError, "not authorized")
	assert.Empty(t, byID[164].VMState)

	require.Len(t, metadata.DiskRecords, 2)
	assert.Equal(t, "POWEROFF", metadata.DiskRecords[0].VMState)
	assert.Equal(t, "UNKNOWN", metadata.DiskRecords[1].LCMState)
}

func TestPoweroffVMHardWaitsForPoweroff(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{161: {name: "node-lost", state: vmStateActive, lcm: lcmUnknown, hasDisk: true}}, 161)
	provider := newFakeOneProvider(t, fake)

	require.NoError(t, provider.PoweroffVMHard(context.Background(), 161, time.Second))
	assert.Equal(t, []string{"one.vm.action poweroff-hard,161"}, fake.calledPrefix("one.vm.action"))
	assert.Equal(t, vmStatePoweroff, fake.vms[161].state)

	// Already POWEROFF: nothing is sent.
	require.NoError(t, provider.PoweroffVMHard(context.Background(), 161, time.Second))
	assert.Len(t, fake.calledPrefix("one.vm.action"), 1)
}

func TestPoweroffVMHardTimesOutWhenTheVMNeverStops(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{161: {name: "node-lost", state: vmStateActive, lcm: lcmUnknown, stuckOff: true}}, 161)
	provider := newFakeOneProvider(t, fake)

	err := provider.PoweroffVMHard(context.Background(), 161, 100*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for VM 161 to reach POWEROFF")
}

func TestDetachVolumeFromStoppedVMDetachesFromPoweroff(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{160: {name: "node-off", state: vmStatePoweroff, hasDisk: true}}, 160)
	provider := newFakeOneProvider(t, fake)

	require.NoError(t, provider.DetachVolumeFromStoppedVM(context.Background(), "pvc-dead", 160, time.Second))
	assert.Equal(t, []string{"one.vm.detach 160,3"}, fake.calledPrefix("one.vm.detach"))
	assert.False(t, fake.vms[160].hasDisk)

	// Idempotent: a second call finds the disk gone and sends nothing.
	require.NoError(t, provider.DetachVolumeFromStoppedVM(context.Background(), "pvc-dead", 160, time.Second))
	assert.Len(t, fake.calledPrefix("one.vm.detach"), 1)
}

func TestDetachVolumeFromStoppedVMRefusesAVMThatIsNotPoweroff(t *testing.T) {
	for name, vm := range map[string]*fakeDeadVM{
		"running": {name: "node-live", state: vmStateActive, lcm: lcmRunning, hasDisk: true},
		"unknown": {name: "node-lost", state: vmStateActive, lcm: lcmUnknown, hasDisk: true},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newDeadOwnerFake(map[int]*fakeDeadVM{160: vm}, 160)
			provider := newFakeOneProvider(t, fake)

			err := provider.DetachVolumeFromStoppedVM(context.Background(), "pvc-dead", 160, time.Second)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "only a POWEROFF VM is detached here")
			assert.Empty(t, fake.calledPrefix("one.vm.detach"))
			assert.True(t, vm.hasDisk)
		})
	}
}

func TestPoweroffVMHardRefusesAVMThatIsNoLongerUnknown(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{161: {name: "node-back", state: vmStateActive, lcm: lcmRunning, hasDisk: true}}, 161)
	provider := newFakeOneProvider(t, fake)

	err := provider.PoweroffVMHard(context.Background(), 161, time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only an UNKNOWN VM is powered off here")
	assert.Empty(t, fake.calledPrefix("one.vm.action"), "a VM whose host came back is never powered off")
}

func TestPoweroffVMHardFailsFastWhenTheVMComesBackRunning(t *testing.T) {
	fake := newDeadOwnerFake(map[int]*fakeDeadVM{161: {name: "node-lost", state: vmStateActive, lcm: lcmUnknown, revives: true}}, 161)
	provider := newFakeOneProvider(t, fake)

	started := time.Now()
	err := provider.PoweroffVMHard(context.Background(), 161, 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not take")
	assert.Less(t, time.Since(started), 2*time.Second, "a failed power-off ends the wait, not the timeout")
}
