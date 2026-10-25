package observatory_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/extensions/observatory/internal/demo"
	"github.com/Leeminjing/go-forest/extensions/observe"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
)

const token = "test-observatory-credential"

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(4 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
func runtime(t *testing.T) *demo.Runtime {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)
	r, err := demo.New(context.Background(), demo.Options{Token: token, SampleInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func request(t *testing.T, url, credential string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	client := http.Client{Timeout: 3 * time.Second}
	r, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func state(t *testing.T, url string) observe.Frame {
	t.Helper()
	r := request(t, url+"/api/v1/state", token)
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatal(r.Status)
	}
	var frame observe.Frame
	if err := json.NewDecoder(r.Body).Decode(&frame); err != nil {
		t.Fatal(err)
	}
	return frame
}

type event struct {
	kind, id string
	data     []byte
}

func stream(t *testing.T, url, after string) (<-chan event, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", url+"/api/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if after != "" {
		req.Header.Set("Last-Event-ID", after)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if r.StatusCode != 200 {
		r.Body.Close()
		cancel()
		t.Fatal(r.Status)
	}
	events := make(chan event, 64)
	go func() {
		defer close(events)
		defer r.Body.Close()
		scanner := bufio.NewScanner(r.Body)
		scanner.Buffer(make([]byte, 4096), 8<<20)
		e := event{}
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if e.kind != "" {
					select {
					case events <- e:
					case <-ctx.Done():
						return
					}
				}
				e = event{}
				continue
			}
			if strings.HasPrefix(line, "event: ") {
				e.kind = strings.TrimPrefix(line, "event: ")
			}
			if strings.HasPrefix(line, "id: ") {
				e.id = strings.TrimPrefix(line, "id: ")
			}
			if strings.HasPrefix(line, "data: ") {
				e.data = []byte(strings.TrimPrefix(line, "data: "))
			}
		}
	}()
	stop := func() { cancel(); r.Body.Close() }
	t.Cleanup(stop)
	return events, stop
}
func frameOf(t *testing.T, e event) observe.Frame {
	t.Helper()
	var f observe.Frame
	if err := json.Unmarshal(e.data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func awaitSnapshot(t *testing.T, events <-chan event, predicate func(observe.Frame) bool) observe.Frame {
	t.Helper()
	timeout := time.NewTimer(4 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case e, ok := <-events:
			if !ok {
				t.Fatal("stream closed")
			}
			if e.kind == "sync" || e.kind == "snapshot" || e.kind == "reset" {
				f := frameOf(t, e)
				if predicate(f) {
					return f
				}
			}
		case <-timeout.C:
			t.Fatal("snapshot timeout")
			return observe.Frame{}
		}
	}
}

func TestHTTPHotReplacementAndLifecycleSamplingWithoutPinnedStream(t *testing.T) {
	r := runtime(t)
	events, _ := stream(t, r.ObservatoryURL, "")
	first := frameOf(t, wait(t, events))
	if first.Snapshot.Counts.Trees != 4 || first.Snapshot.Counts.Links != 1 || first.Snapshot.Counts.NestBindings != 1 {
		t.Fatal(first)
	}
	old := make(chan map[string]string, 1)
	failure := make(chan error, 1)
	go func() {
		resp, err := http.Get(r.AppURL + "/work?hold=1")
		if err != nil {
			failure <- err
			return
		}
		defer resp.Body.Close()
		var body map[string]string
		err = json.NewDecoder(resp.Body).Decode(&body)
		old <- body
		failure <- err
	}()
	wait(t, r.Entered)
	if _, err := r.Replace(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitSnapshot(t, events, func(f observe.Frame) bool { return f.Snapshot.Generation == 2 && f.Snapshot.Counts.Draining == 1 })
	resp := request(t, r.AppURL+"/work", "")
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if body["value"] != "v2" || body["again"] != "v2" {
		t.Fatal(body)
	}
	r.Release()
	body = wait(t, old)
	if body["value"] != "v1" || body["again"] != "v1" || body["generation"] != "1" {
		t.Fatal(body)
	}
	if err := wait(t, failure); err != nil {
		t.Fatal(err)
	}
	awaitSnapshot(t, events, func(f observe.Frame) bool {
		for _, i := range f.Snapshot.Instances {
			if i.TreeID == "business" && i.Version == "1" && i.State == "stopped" {
				return true
			}
		}
		return false
	})
	// SSE remains open while Close runs. It must not pin the graph or block Stop.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAuthenticationCursorRecoveryAndReadOnlyRoutes(t *testing.T) {
	r := runtime(t)
	for _, credential := range []string{"", "incorrect"} {
		resp := request(t, r.ObservatoryURL+"/api/v1/state", credential)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatal(resp.Status)
		}
	}
	initial := state(t, r.ObservatoryURL)
	for n := 0; n < 20; n++ {
		_, err := r.Forest.Apply(context.Background(), forest.ChangePlan{BaseGeneration: r.Forest.Snapshot().Generation})
		if err != nil {
			t.Fatal(err)
		}
	}
	events, stop := stream(t, r.ObservatoryURL, initial.Cursor)
	reset := wait(t, events)
	if reset.kind != "reset" || frameOf(t, reset).ResetReason != "history_gap" {
		t.Fatal(reset)
	}
	stop()
	fresh := state(t, r.ObservatoryURL)
	events, stop = stream(t, r.ObservatoryURL, fresh.Cursor)
	if e := wait(t, events); e.kind != "snapshot" || e.id != "" {
		t.Fatal(e)
	}
	stop()
	boot := strings.Repeat("0", 32)
	events, stop = stream(t, r.ObservatoryURL, boot+":0")
	if e := wait(t, events); e.kind != "reset" || frameOf(t, e).ResetReason != "runtime_changed" {
		t.Fatal(e)
	}
	stop()
	c, _ := observe.ParseCursor(fresh.Cursor)
	for _, v := range []string{"bad", c.BootID + ":18446744073709551616", c.BootID + ":999999"} {
		resp := request(t, r.ObservatoryURL+"/api/v1/state?cursor="+v, token)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatal(v, resp.Status)
		}
	}
	req, _ := http.NewRequest("POST", r.ObservatoryURL+"/api/v1/state", strings.NewReader(`{"operations":[]}`))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal(resp.Status)
	}
	req, _ = http.NewRequest("GET", r.ObservatoryURL+"/api/v1/state", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Origin", "http://other.invalid")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal(resp.Status)
	}
	page := request(t, r.ObservatoryURL+"/", "")
	data, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if !strings.Contains(string(data), "Forest Observatory") || strings.Contains(string(data), token) {
		t.Fatal("invalid static page")
	}
}

func TestTwoPlacementsAndRemovalTerminatesStream(t *testing.T) {
	f := forest.New(forest.Options{})
	t.Cleanup(func() { _ = f.Close(context.Background()) })
	b, err := observatory.New(f, observatory.Options{Token: token, SampleInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range b.Definitions {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := b.Blueprint.Instantiate("left", "")
	right, _ := b.Blueprint.Instantiate("right", "")
	plan := a.Plan(0)
	plan.Operations = append(plan.Operations, right.Plan(0).Operations...)
	if _, err = f.Apply(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	endpoint := func(id forest.TreeID) string {
		var value string
		err := f.Invoke(context.Background(), func(ctx context.Context) error {
			return forest.Use[ginforest.Endpoint](ctx, f, id, "http", func(e ginforest.Endpoint) error { value = e.URL(); return nil })
		})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	leftURL, rightURL := endpoint("left"), endpoint("right")
	if leftURL == rightURL {
		t.Fatal("shared listener")
	}
	if state(t, leftURL).BootID == state(t, rightURL).BootID {
		t.Fatal("shared session")
	}
	events, _ := stream(t, leftURL, "")
	wait(t, events)
	_, err = f.Apply(context.Background(), forest.ChangePlan{BaseGeneration: 1, Operations: []forest.Operation{forest.RemoveSubtree("left")}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range events {
		}
		close(done)
	}()
	wait(t, done)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = f.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if state(t, rightURL).Snapshot.Counts.Trees != 2 {
		t.Fatal("remaining placement damaged")
	}
}

func TestConfigurationRejectsExposedAddresses(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	for _, address := range []string{"0.0.0.0:0", ":0", "localhost:0", "192.0.2.1:80"} {
		if _, err := observatory.New(f, observatory.Options{Token: token, Address: address}); err == nil {
			t.Fatal(address)
		}
	}
	if _, err := observatory.New(f, observatory.Options{Token: "short"}); err == nil {
		t.Fatal("accepted missing credential")
	}
}

func TestRestartOnSameAddressesResetsOldCursor(t *testing.T) {
	r := runtime(t)
	old := state(t, r.ObservatoryURL)
	address := strings.TrimPrefix(r.ObservatoryURL, "http://")
	appAddress := strings.TrimPrefix(r.AppURL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := demo.New(context.Background(), demo.Options{Token: token, AppAddress: appAddress, ObservatoryAddress: address, SampleInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = next.Close(ctx)
	})
	events, _ := stream(t, next.ObservatoryURL, old.Cursor)
	e := wait(t, events)
	frame := frameOf(t, e)
	if e.kind != "reset" || frame.ResetReason != "runtime_changed" || frame.BootID == old.BootID {
		t.Fatal(frame)
	}
}
