package opennebula

import (
	"context"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOneRPC is a minimal in-memory OpenNebula XML-RPC endpoint: one image,
// optionally attached as one disk of one VM. It records every call so a test
// can assert which OpenNebula operations ran.
type fakeOneRPC struct {
	mu sync.Mutex

	imageID     int
	imageName   string
	imageSizeMB int
	template    map[string]string

	vmID       int
	diskID     int
	diskSizeMB int
	attached   bool
	resizeErr  string
	resizeArgs []string
	calls      []string
}

type xmlRPCCall struct {
	MethodName string `xml:"methodName"`
	Params     []struct {
		Value struct {
			Inner string `xml:",innerxml"`
		} `xml:"value"`
	} `xml:"params>param"`
}

var xmlRPCScalar = regexp.MustCompile(`(?s)^\s*<(\w+)>(.*)</\w+>\s*$`)

func xmlRPCParam(inner string) string {
	if m := xmlRPCScalar.FindStringSubmatch(inner); m != nil {
		return html.UnescapeString(m[2])
	}
	return html.UnescapeString(inner)
}

var templatePair = regexp.MustCompile(`(?m)^\s*([A-Z0-9_]+)\s*=\s*"((?:[^"\\]|\\.)*)"\s*$`)

func (f *fakeOneRPC) imageXML() string {
	keys := make([]string, 0, len(f.template))
	for key := range f.template {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var tpl strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&tpl, "<%s>%s</%s>", key, html.EscapeString(f.template[key]), key)
	}
	return fmt.Sprintf("<IMAGE><ID>%d</ID><NAME>%s</NAME><SIZE>%d</SIZE><STATE>1</STATE><TEMPLATE>%s</TEMPLATE></IMAGE>",
		f.imageID, f.imageName, f.imageSizeMB, tpl.String())
}

func (f *fakeOneRPC) vmXML() string {
	disk := ""
	if f.attached {
		disk = fmt.Sprintf("<DISK><DISK_ID>%d</DISK_ID><IMAGE_ID>%d</IMAGE_ID><SIZE>%d</SIZE></DISK>", f.diskID, f.imageID, f.diskSizeMB)
	}
	return fmt.Sprintf("<VM><ID>%d</ID><NAME>node-%d</NAME><STATE>3</STATE><LCM_STATE>3</LCM_STATE><TEMPLATE>%s</TEMPLATE></VM>", f.vmID, f.vmID, disk)
}

func (f *fakeOneRPC) handle(call xmlRPCCall) (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call.MethodName)

	params := make([]string, 0, len(call.Params))
	for _, p := range call.Params {
		params = append(params, xmlRPCParam(p.Value.Inner))
	}

	switch call.MethodName {
	case "one.imagepool.info":
		return true, "<IMAGE_POOL>" + f.imageXML() + "</IMAGE_POOL>"
	case "one.image.info":
		return true, f.imageXML()
	case "one.image.update":
		// params: token, id, template, merge type (1 = merge)
		for _, m := range templatePair.FindAllStringSubmatch(params[2], -1) {
			f.template[m[1]] = m[2]
		}
		return true, strconv.Itoa(f.imageID)
	case "one.vmpool.info":
		if !f.attached {
			return true, "<VM_POOL></VM_POOL>"
		}
		return true, "<VM_POOL>" + f.vmXML() + "</VM_POOL>"
	case "one.vm.info":
		return true, f.vmXML()
	case "one.vm.diskresize":
		f.resizeArgs = append(f.resizeArgs, strings.Join(params[1:], ","))
		if f.resizeErr != "" {
			return false, f.resizeErr
		}
		size, err := strconv.Atoi(params[3])
		if err != nil {
			return false, "bad size"
		}
		f.diskSizeMB = size
		return true, strconv.Itoa(f.vmID)
	}
	return false, "unexpected method " + call.MethodName
}

func (f *fakeOneRPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var call xmlRPCCall
	if err := xml.Unmarshal(body, &call); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ok, payload := f.handle(call)
	okValue := 0
	if ok {
		okValue = 1
	}
	errCode := 0
	if !ok {
		errCode = 4096
	}
	_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><methodResponse><params><param><value><array><data>`+
		`<value><boolean>%d</boolean></value><value><string>%s</string></value><value><i4>%d</i4></value>`+
		`</data></array></value></param></params></methodResponse>`, okValue, html.EscapeString(payload), errCode)
}

func (f *fakeOneRPC) called(method string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == method {
			return true
		}
	}
	return false
}

func newFakeOneProvider(t *testing.T, fake *fakeOneRPC) *PersistentDiskVolumeProvider {
	t.Helper()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	client := &OpenNebulaClient{Client: goca.NewDefaultClient(goca.OneConfig{Token: "user:pass", Endpoint: server.URL})}
	provider, err := NewPersistentDiskVolumeProvider(client, HotplugTimeoutPolicy{
		BaseTimeout:  5 * time.Second,
		MaxTimeout:   5 * time.Second,
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	return provider
}

func newDetachedFake() *fakeOneRPC {
	return &fakeOneRPC{
		imageID:     7,
		imageName:   "pvc-detached",
		imageSizeMB: 1024,
		template:    map[string]string{},
		vmID:        42,
		diskID:      1,
		diskSizeMB:  1024,
	}
}

func TestExpandDetachedVolumeDefersToNextAttach(t *testing.T) {
	fake := newDetachedFake()
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()
	requested := int64(2048) * sizeConversion

	newSize, err := provider.ExpandVolume(ctx, fake.imageName, requested, true)
	require.NoError(t, err)
	assert.Equal(t, requested, newSize)

	assert.Equal(t, strconv.FormatInt(requested, 10), fake.template[csiRequestedSizeBytesTag])
	assert.Equal(t, "2048", fake.template[csiRequestedSizeMBTag])
	assert.Equal(t, csiExpandedByValue, fake.template[csiExpandedByTag])
	assert.NotEmpty(t, fake.template[csiExpandedAtTag])
	assert.False(t, fake.called("one.vm.diskresize"), "a detached expand must not resize through a VM")
	assert.Equal(t, 1024, fake.imageSizeMB, "the canonical image size is left alone")

	// The requested size is now the volume's size everywhere the driver asks.
	_, reported, err := provider.VolumeExists(ctx, fake.imageName)
	require.NoError(t, err)
	assert.Equal(t, requested, int64(reported))
	resolved, err := provider.ResolveVolumeSizeBytes(ctx, fake.imageName)
	require.NoError(t, err)
	assert.Equal(t, requested, resolved)

	// A retried expand to the same size returns early without another update.
	fake.calls = nil
	again, err := provider.ExpandVolume(ctx, fake.imageName, requested, true)
	require.NoError(t, err)
	assert.Equal(t, requested, again)
	assert.False(t, fake.called("one.image.update"))
}

func TestExpandDetachedVolumeRefusedWhenGateDisabled(t *testing.T) {
	fake := newDetachedFake()
	provider := newFakeOneProvider(t, fake)

	newSize, err := provider.ExpandVolume(context.Background(), fake.imageName, int64(2048)*sizeConversion, false)
	require.Error(t, err)
	assert.Zero(t, newSize)
	assert.True(t, IsDatastoreConfigError(err))
	assert.Contains(t, err.Error(), "detachedDiskExpansion feature gate")
	assert.Empty(t, fake.template, "a refused expand must not write metadata")
	assert.False(t, fake.called("one.image.update"))
	assert.False(t, fake.called("one.vm.diskresize"))
}

func TestAttachAfterDeferredExpandRestoresRequestedSize(t *testing.T) {
	fake := newDetachedFake()
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()
	requested := int64(2048) * sizeConversion

	_, err := provider.ExpandVolume(ctx, fake.imageName, requested, true)
	require.NoError(t, err)

	// The next attach hands the VM the canonical (old) size.
	fake.attached = true
	require.NoError(t, provider.restoreAttachedVolumeRequestedSize(ctx, fake.imageName, "node-42", fake.imageID, fake.vmID, 5*time.Second))
	assert.Equal(t, []string{"42,1,2048"}, fake.resizeArgs)
	assert.Equal(t, 2048, fake.diskSizeMB)
}

func TestAttachAfterDeferredExpandFailsWhenRestoreResizeFails(t *testing.T) {
	fake := newDetachedFake()
	provider := newFakeOneProvider(t, fake)
	ctx := context.Background()

	_, err := provider.ExpandVolume(ctx, fake.imageName, int64(2048)*sizeConversion, true)
	require.NoError(t, err)

	fake.attached = true
	fake.resizeErr = "[one.vm.diskresize] not enough space"
	err = provider.restoreAttachedVolumeRequestedSize(ctx, fake.imageName, "node-42", fake.imageID, fake.vmID, 5*time.Second)
	require.Error(t, err, "a failed restore must fail the attach, never hand the node a smaller disk")
	assert.Contains(t, err.Error(), "failed to restore expanded volume")
}
