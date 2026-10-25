package observatory_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	"github.com/Leeminjing/go-forest/extensions/observatory"
	"github.com/Leeminjing/go-forest/forest"
)

type runtimeSource string

func (s runtimeSource) ReadRuntime(context.Context) (json.RawMessage, error) {
	return json.RawMessage(s), nil
}
func TestRuntimeObservationIsOptionalAuthorizedAndBounded(t *testing.T) {
	for _, source := range []string{"", `{"coverage":"explicit","operations":[]}`, "not-json", `{"padding":"` + strings.Repeat("x", 256<<10) + `"}`} {
		t.Run(strings.TrimSpace(source)[:min(len(strings.TrimSpace(source)), 12)], func(t *testing.T) {
			f := forest.New(forest.Options{})
			defer f.Close(context.Background())
			b, err := observatory.New(f, observatory.Options{Token: token})
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range b.Definitions {
				if err = f.Register(d); err != nil {
					t.Fatal(err)
				}
			}
			p, err := b.Blueprint.Instantiate("obs", "")
			if err != nil {
				t.Fatal(err)
			}
			plan := p.Plan(0)
			if source != "" {
				d := forest.Definition{TypeID: "runtime", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[runtimeSource]("read", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
					return forest.Hooks{}, forest.Provide(e, "read", runtimeSource(source))
				}}
				if err = f.Register(d); err != nil {
					t.Fatal(err)
				}
				plan.Operations = append(plan.Operations, forest.AddRoot("runtime", d.Ref()), forest.Connect(forest.Link{ID: "runtime", Consumer: "obs", Port: "runtime", Provider: "runtime", Capability: "read"}))
			}
			if _, err = f.Apply(context.Background(), plan); err != nil {
				t.Fatal(err)
			}
			var url string
			err = f.Invoke(context.Background(), func(ctx context.Context) error {
				return forest.Use[ginforest.Endpoint](ctx, f, "obs", "http", func(e ginforest.Endpoint) error { url = e.URL(); return nil })
			})
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.Get(url + "/api/v1/runtime")
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 401 {
				t.Fatal("runtime endpoint bypassed authorization")
			}
			req, _ := http.NewRequest("GET", url+"/api/v1/runtime", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			want := 503
			if source == `{"coverage":"explicit","operations":[]}` {
				want = 200
			}
			if resp.StatusCode != want {
				t.Fatalf("status %d want %d body=%s", resp.StatusCode, want, data)
			}
		})
	}
}
