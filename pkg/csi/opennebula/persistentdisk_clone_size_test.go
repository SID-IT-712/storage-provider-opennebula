package opennebula

import (
	"context"
	"fmt"
	"html"
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

// fakeCloneImage is one image held by fakeOnePool.
type fakeCloneImage struct {
	id       int
	name     string
	sizeMB   int
	template map[string]string
}

// fakeOnePool is an in-memory OpenNebula XML-RPC endpoint with several images,
// one image datastore and one VM, enough to clone an image and attach the clone.
// one.image.clone behaves like OpenNebula's Image::clone_template
// (src/image/Image.cc): the clone gets a copy of the source's template and the
// source's canonical SIZE, never the size the disk was grown to.
type fakeOnePool struct {
	mu sync.Mutex

	images      map[int]*fakeCloneImage
	nextImageID int
	freeMB      int

	vmID       int
	diskID     int
	diskImage  int
	diskSizeMB int
	resizeArgs []string
	calls      []string
}

func newFakeOnePool(source *fakeCloneImage) *fakeOnePool {
	return &fakeOnePool{
		images:      map[int]*fakeCloneImage{source.id: source},
		nextImageID: source.id + 1,
		freeMB:      1024 * 1024,
		vmID:        42,
		diskID:      1,
		diskImage:   -1,
	}
}

func (f *fakeOnePool) imageXML(image *fakeCloneImage) string {
	keys := make([]string, 0, len(image.template))
	for key := range image.template {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var tpl strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&tpl, "<%s>%s</%s>", key, html.EscapeString(image.template[key]), key)
	}
	return fmt.Sprintf("<IMAGE><ID>%d</ID><NAME>%s</NAME><SIZE>%d</SIZE><STATE>1</STATE><TEMPLATE>%s</TEMPLATE></IMAGE>",
		image.id, image.name, image.sizeMB, tpl.String())
}

func (f *fakeOnePool) vmXML() string {
	disk := ""
	if f.diskImage >= 0 {
		disk = fmt.Sprintf("<DISK><DISK_ID>%d</DISK_ID><IMAGE_ID>%d</IMAGE_ID><SIZE>%d</SIZE></DISK>", f.diskID, f.diskImage, f.diskSizeMB)
	}
	return fmt.Sprintf("<VM><ID>%d</ID><NAME>node-%d</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE>%s</TEMPLATE></VM>", f.vmID, f.vmID, disk)
}

func (f *fakeOnePool) imageParam(raw string) (*fakeCloneImage, bool) {
	id, err := strconv.Atoi(raw)
	if err != nil {
		return nil, false
	}
	image, ok := f.images[id]
	return image, ok
}

func (f *fakeOnePool) handle(call xmlRPCCall) (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call.MethodName)

	params := make([]string, 0, len(call.Params))
	for _, p := range call.Params {
		params = append(params, xmlRPCParam(p.Value.Inner))
	}

	switch call.MethodName {
	case "one.imagepool.info":
		ids := make([]int, 0, len(f.images))
		for id := range f.images {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		var pool strings.Builder
		for _, id := range ids {
			pool.WriteString(f.imageXML(f.images[id]))
		}
		return true, "<IMAGE_POOL>" + pool.String() + "</IMAGE_POOL>"
	case "one.image.info":
		image, ok := f.imageParam(params[1])
		if !ok {
			return false, "image not found"
		}
		return true, f.imageXML(image)
	case "one.image.clone":
		// params: token, source id, clone name, datastore id
		source, ok := f.imageParam(params[1])
		if !ok {
			return false, "image not found"
		}
		template := make(map[string]string, len(source.template))
		for key, value := range source.template {
			template[key] = value
		}
		clone := &fakeCloneImage{id: f.nextImageID, name: params[2], sizeMB: source.sizeMB, template: template}
		f.images[clone.id] = clone
		f.nextImageID++
		return true, strconv.Itoa(clone.id)
	case "one.image.update":
		// params: token, id, template, merge type (1 = merge)
		image, ok := f.imageParam(params[1])
		if !ok {
			return false, "image not found"
		}
		for _, m := range templatePair.FindAllStringSubmatch(params[2], -1) {
			image.template[m[1]] = m[2]
		}
		return true, strconv.Itoa(image.id)
	case "one.datastorepool.info":
		return true, fmt.Sprintf("<DATASTORE_POOL><DATASTORE><ID>100</ID><NAME>local-images</NAME><DS_MAD>fs</DS_MAD><TM_MAD>local</TM_MAD>"+
			"<TYPE>0</TYPE><STATE>0</STATE><FREE_MB>%d</FREE_MB><TOTAL_MB>%d</TOTAL_MB><TEMPLATE></TEMPLATE></DATASTORE></DATASTORE_POOL>", f.freeMB, f.freeMB)
	case "one.vmpool.info":
		if f.diskImage < 0 {
			return true, "<VM_POOL></VM_POOL>"
		}
		return true, "<VM_POOL>" + f.vmXML() + "</VM_POOL>"
	case "one.vm.info":
		return true, f.vmXML()
	case "one.vm.diskresize":
		f.resizeArgs = append(f.resizeArgs, strings.Join(params[1:], ","))
		size, err := strconv.Atoi(params[3])
		if err != nil {
			return false, "bad size"
		}
		f.diskSizeMB = size
		return true, strconv.Itoa(f.vmID)
	}
	return false, "unexpected method " + call.MethodName
}

func (f *fakeOnePool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serveFakeXMLRPC(w, r, f.handle)
}

func (f *fakeOnePool) imageByName(t *testing.T, name string) *fakeCloneImage {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, image := range f.images {
		if image.name == name {
			return image
		}
	}
	t.Fatalf("no image named %q", name)
	return nil
}

func (f *fakeOnePool) called(method string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == method {
			return true
		}
	}
	return false
}

// attach puts the image on the VM the way OpenNebula does: the VM disk starts
// at the image's canonical SIZE.
func (f *fakeOnePool) attach(image *fakeCloneImage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diskImage = image.id
	f.diskSizeMB = image.sizeMB
}

var cloneTestSelection = DatastoreSelectionConfig{
	Identifiers:  []string{"100"},
	Policy:       DatastoreSelectionPolicyLeastUsed,
	AllowedTypes: []string{"local"},
}

func newCloneSource() *fakeCloneImage {
	return &fakeCloneImage{id: 7, name: "pvc-source", sizeMB: 1024, template: map[string]string{}}
}

func TestCloneOfGrownVolumeKeepsGrownSize(t *testing.T) {
	fake := newFakeOnePool(newCloneSource())
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()
	grown := int64(2048) * sizeConversion

	// Grow the source while detached (fix 3): canonical SIZE stays 1024 MiB.
	_, err := provider.ExpandVolume(ctx, "pvc-source", grown, true)
	require.NoError(t, err)

	// Kubernetes requests a clone at least as large as the source PVC.
	result, err := provider.CloneVolume(ctx, "pvc-clone", "pvc-source", grown, cloneTestSelection)
	require.NoError(t, err)
	assert.Equal(t, grown, result.CapacityBytes, "the clone's CSI capacity is the source's grown size, not its canonical size")

	clone := fake.imageByName(t, "pvc-clone")
	assert.Equal(t, 1024, clone.sizeMB, "OpenNebula clones at the canonical size")
	assert.Equal(t, strconv.FormatInt(grown, 10), clone.template[csiRequestedSizeBytesTag])
	assert.Equal(t, "2048", clone.template[csiRequestedSizeMBTag])

	_, reported, err := provider.VolumeExists(ctx, "pvc-clone")
	require.NoError(t, err)
	assert.Equal(t, grown, int64(reported))

	// The next attach grows the clone's disk to the grown size.
	fake.attach(clone)
	require.NoError(t, provider.restoreAttachedVolumeRequestedSize(ctx, "pvc-clone", "node-42", clone.id, fake.vmID, 5*time.Second))
	assert.Equal(t, []string{"42,1,2048"}, fake.resizeArgs)
	assert.Equal(t, 2048, fake.diskSizeMB)
}

func TestCloneLargerThanSourceRecordsRequestedSize(t *testing.T) {
	fake := newFakeOnePool(newCloneSource())
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()
	requested := int64(4096) * sizeConversion

	result, err := provider.CloneVolume(ctx, "pvc-clone", "pvc-source", requested, cloneTestSelection)
	require.NoError(t, err)
	assert.Equal(t, requested, result.CapacityBytes)

	clone := fake.imageByName(t, "pvc-clone")
	assert.Equal(t, strconv.FormatInt(requested, 10), clone.template[csiRequestedSizeBytesTag])
	assert.Equal(t, "4096", clone.template[csiRequestedSizeMBTag])
	assert.Empty(t, fake.imageByName(t, "pvc-source").template, "the source is never modified")

	_, reported, err := provider.VolumeExists(ctx, "pvc-clone")
	require.NoError(t, err)
	assert.Equal(t, requested, int64(reported))

	fake.attach(clone)
	require.NoError(t, provider.restoreAttachedVolumeRequestedSize(ctx, "pvc-clone", "node-42", clone.id, fake.vmID, 5*time.Second))
	assert.Equal(t, []string{"42,1,4096"}, fake.resizeArgs)
	assert.Equal(t, 4096, fake.diskSizeMB)
}

func TestCloneChecksDatastoreCapacityForCloneSize(t *testing.T) {
	fake := newFakeOnePool(newCloneSource())
	fake.freeMB = 3000
	provider := newFakeOneProvider(t, fake)

	_, err := provider.CloneVolume(context.Background(), "pvc-clone", "pvc-source", int64(4096)*sizeConversion, cloneTestSelection)
	require.Error(t, err)
	assert.True(t, IsDatastoreCapacityError(err))
	assert.False(t, fake.called("one.image.clone"), "a clone that cannot grow to its size must not be created")
}

func TestCloneOfUngrownVolumeIsUnchanged(t *testing.T) {
	fake := newFakeOnePool(newCloneSource())
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()
	size := int64(1024) * sizeConversion

	result, err := provider.CloneVolume(ctx, "pvc-clone", "pvc-source", size, cloneTestSelection)
	require.NoError(t, err)
	assert.Equal(t, size, result.CapacityBytes)
	assert.False(t, fake.called("one.image.update"), "a clone at the source's size needs no metadata")

	clone := fake.imageByName(t, "pvc-clone")
	assert.Empty(t, clone.template)

	fake.attach(clone)
	require.NoError(t, provider.restoreAttachedVolumeRequestedSize(ctx, "pvc-clone", "node-42", clone.id, fake.vmID, 5*time.Second))
	assert.Empty(t, fake.resizeArgs)
}
