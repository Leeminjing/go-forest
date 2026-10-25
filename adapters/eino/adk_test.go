package einoforest_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type offlineModel struct{}

func (offlineModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return offlineModel{}, nil
}
func (offlineModel) Generate(ctx context.Context, m []*schema.Message, _ ...model.Option) (out *schema.Message, err error) {
	err = ef.Guard(ctx, func() error {
		if len(m) == 0 {
			return ef.ErrInvalid
		}
		if last := m[len(m)-1]; last.Role == schema.Tool {
			out = schema.AssistantMessage("answer:"+last.Content, nil)
		} else {
			out = schema.AssistantMessage("", []schema.ToolCall{{ID: "call-1", Function: schema.FunctionCall{Name: "lookup", Arguments: `{"key":"oak"}`}}})
		}
		return nil
	})
	return
}
func (m offlineModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, err := m.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{out}), nil
}

type nativeTool struct {
	interrupt        bool
	label            string
	calls            *atomic.Int32
	entered, release chan struct{}
}

func (nativeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: "lookup", Desc: "deterministic lookup", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"key": {Type: schema.String, Required: true}})}, nil
}
func (t nativeTool) InvokableRun(ctx context.Context, _ string, _ ...tool.Option) (out string, err error) {
	err = ef.Guard(ctx, func() error {
		if t.entered != nil {
			close(t.entered)
			<-t.release
		}
		if t.interrupt {
			was, _, _ := tool.GetInterruptState[string](ctx)
			if !was {
				return tool.StatefulInterrupt(ctx, "approve", "oak")
			}
			_, has, data := tool.GetResumeContext[string](ctx)
			if has {
				out = data
				return nil
			}
		}
		if t.calls != nil {
			t.calls.Add(1)
		}
		out = t.label
		return nil
	})
	return
}
func nativeDefinition(cfg ef.Config, directory string, t nativeTool) (forest.Definition, error) {
	return ef.ADKDefinition(ef.ADKOptions{Config: cfg, Build: func(context.Context, *forest.BuildEnv) (*ef.ADKHost, error) {
		store, err := ef.NewFileStore(directory, ef.Profile{Engine: "eino/0.9.22", Namespace: cfg.Namespace, Schema: "lookup-v1"})
		if err != nil {
			return nil, err
		}
		return &ef.ADKHost{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.ADKSession, error) {
			a, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{Name: "native", Description: "offline native engine", Model: offlineModel{}, ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{t}}}, MaxIterations: 4})
			if err != nil {
				return ef.ADKSession{}, err
			}
			return ef.ADKSession{Runner: adk.NewRunner(ctx, adk.RunnerConfig{Agent: a, EnableStreaming: true, CheckPointStore: store}), Lifetime: ef.ExecutionFrom(ctx)}, nil
		}}, nil
	}})
}
func mount(t *testing.T, f *forest.Forest, defs ...forest.Definition) {
	t.Helper()
	for _, d := range defs {
		if err := f.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Apply(context.Background(), forest.ChangePlan{Operations: []forest.Operation{forest.AddRoot("agent", defs[0].Ref())}}); err != nil {
		t.Fatal(err)
	}
}
func invokeADK(f *forest.Forest, fn func(context.Context, ef.ADK) error) error {
	return f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[ef.ADK](ctx, f, "agent", "agent", func(a ef.ADK) error { return fn(ctx, a) })
	})
}
func TestNativeADKToolCycleAndCheckpointResume(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 8)
	var calls atomic.Int32
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "native", Owner: owner}, t.TempDir(), nativeTool{interrupt: true, label: "oak", calls: &calls})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	var interrupted ef.Result
	var answer string
	var address string
	visit := func(e *adk.AgentEvent) error {
		if e.Action != nil && e.Action.Interrupted != nil {
			for _, ic := range e.Action.Interrupted.InterruptContexts {
				address = ic.ID
			}
		}
		m, err := ef.ReadMessage(e)
		if m != nil && m.Role == schema.Assistant && m.Content != "" {
			answer = m.Content
		}
		return err
	}
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		var err error
		interrupted, err = a.Query(ctx, "lookup", ef.RunOptions{CheckpointID: "run-1"}, visit)
		return err
	})
	if err != nil || !interrupted.Interrupted || address == "" {
		t.Fatalf("interrupt=%+v address=%s err=%v", interrupted, address, err)
	}
	if calls.Load() != 0 {
		t.Fatal("tool executed before approval")
	}
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		result, err := a.Resume(ctx, &adk.ResumeParams{Targets: map[string]any{address: "approved"}}, ef.RunOptions{CheckpointID: "run-1"}, visit)
		if result.Interrupted {
			t.Error("resume remained interrupted")
		}
		return err
	})
	if err != nil || answer != "answer:approved" {
		t.Fatalf("answer=%s err=%v", answer, err)
	}
	if err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Resume(ctx, nil, ef.RunOptions{CheckpointID: "absent"}, visit)
		return err
	}); err == nil {
		t.Fatal("unknown checkpoint restarted task")
	}
}
func TestNativeADKConsumerErrorAndEventError(t *testing.T) {
	f := forest.New(forest.Options{})
	defer f.Close(context.Background())
	owner, _ := ef.NewOwner(f, 8)
	d, err := nativeDefinition(ef.Config{TypeID: "native", Version: "1", Namespace: "n", Owner: owner}, t.TempDir(), nativeTool{label: "oak"})
	if err != nil {
		t.Fatal(err)
	}
	mount(t, f, d)
	sentinel := errors.New("visitor failed")
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Query(ctx, "query", ef.RunOptions{CheckpointID: "one"}, func(*adk.AgentEvent) error { return sentinel })
		return err
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	var answer strings.Builder
	err = invokeADK(f, func(ctx context.Context, a ef.ADK) error {
		_, err := a.Run(ctx, []*schema.Message{schema.UserMessage("lookup")}, ef.RunOptions{CheckpointID: "two"}, func(e *adk.AgentEvent) error {
			m, err := ef.ReadMessage(e)
			if m != nil {
				answer.WriteString(m.Content)
			}
			return err
		})
		return err
	})
	if err != nil || !strings.Contains(answer.String(), "answer:oak") {
		t.Fatalf("answer=%s err=%v", answer.String(), err)
	}
}
