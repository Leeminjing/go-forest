package einodemo

import (
	"context"
	"encoding/json"
	"errors"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type lookup interface {
	Lookup(context.Context, string) (string, error)
}
type input struct {
	Key string `json:"key"`
}
type nativeModel struct{}

func (nativeModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return nativeModel{}, nil
}
func (nativeModel) Generate(ctx context.Context, m []*schema.Message, _ ...model.Option) (out *schema.Message, err error) {
	err = ef.Guard(ctx, func() error {
		if len(m) == 0 {
			return errors.New("missing messages")
		}
		last := m[len(m)-1]
		if last.Role == schema.Tool {
			out = schema.AssistantMessage(last.Content, nil)
		} else {
			args, _ := json.Marshal(input{Key: last.Content})
			out = schema.AssistantMessage("", []schema.ToolCall{{ID: "lookup-1", Function: schema.FunctionCall{Name: "lookup", Arguments: string(args)}}})
		}
		return nil
	})
	return
}
func (m nativeModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	result, err := m.Generate(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.Message{result}), nil
}

type nativeTool struct{ ref forest.Ref[lookup] }

func (nativeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: "lookup", Desc: "read shared catalog twice", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"key": {Type: schema.String, Required: true}})}, nil
}
func (t nativeTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (out string, err error) {
	err = ef.Guard(ctx, func() error {
		var in input
		if len(args) > 4096 || json.Unmarshal([]byte(args), &in) != nil || in.Key == "" {
			return errors.New("invalid lookup arguments")
		}
		return t.ref.Use(ctx, func(r lookup) error {
			one, err := r.Lookup(ctx, in.Key)
			if err != nil {
				return err
			}
			two, err := r.Lookup(ctx, in.Key)
			out = one + "/" + two
			return err
		})
	})
	return
}
func nativeDefinition(owner *ef.Owner, directory, version string) (forest.Definition, error) {
	return ef.ADKDefinition(ef.ADKOptions{Config: ef.Config{TypeID: "eino-example.native", Version: version, Namespace: "native", Owner: owner, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[lookup]("lookup", forest.Required())}}, Build: func(_ context.Context, env *forest.BuildEnv) (*ef.ADKHost, error) {
		ref, err := forest.Reference[lookup](env, "lookup")
		if err != nil {
			return nil, err
		}
		store, err := ef.NewFileStore(directory, ef.Profile{Engine: "eino/0.9.22", Namespace: "native", Schema: "lookup-v1"})
		if err != nil {
			return nil, err
		}
		return &ef.ADKHost{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.ADKSession, error) {
			a, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{Name: "native", Description: "deterministic tool agent", Model: nativeModel{}, ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{nativeTool{ref}}}}, MaxIterations: 4})
			if err != nil {
				return ef.ADKSession{}, err
			}
			return ef.ADKSession{Runner: adk.NewRunner(ctx, adk.RunnerConfig{Agent: a, EnableStreaming: true, CheckPointStore: store}), Lifetime: ef.ExecutionFrom(ctx)}, nil
		}}, nil
	}})
}
func graphDefinition(owner *ef.Owner) (forest.Definition, error) {
	return ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "eino-example.graph", Version: "1", Namespace: "graph", Owner: owner, Ports: []forest.PortDescriptor{forest.Port[lookup]("lookup", forest.Required())}}, Build: func(ctx context.Context, env *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		ref, err := forest.Reference[lookup](env, "lookup")
		if err != nil {
			return nil, err
		}
		g := compose.NewGraph[string, string]()
		if err = g.AddLambdaNode("lookup", compose.InvokableLambda(func(ctx context.Context, in string) (out string, err error) {
			err = ef.Guard(ctx, func() error { return ref.Use(ctx, func(r lookup) error { out, err = r.Lookup(ctx, in); return err }) })
			return
		})); err != nil {
			return nil, err
		}
		for _, name := range []string{"format", "other"} {
			label := name
			if err = g.AddLambdaNode(name, compose.InvokableLambda(func(ctx context.Context, in string) (out string, err error) {
				err = ef.Guard(ctx, func() error { out = label + ":" + in; return nil })
				return
			})); err != nil {
				return nil, err
			}
			if err = g.AddEdge(name, compose.END); err != nil {
				return nil, err
			}
		}
		if err = g.AddEdge(compose.START, "lookup"); err != nil {
			return nil, err
		}
		if err = g.AddBranch("lookup", compose.NewGraphBranch(func(context.Context, string) (string, error) { return "format", nil }, map[string]bool{"format": true, "other": true})); err != nil {
			return nil, err
		}
		r, err := g.Compile(ctx)
		if err != nil {
			return nil, err
		}
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{}, NewSession: func(ctx context.Context) (ef.GraphSession[string, string], error) {
			return ef.GraphSession[string, string]{Runnable: r, Lifetime: ef.ExecutionFrom(ctx)}, nil
		}}, nil
	}})
}
