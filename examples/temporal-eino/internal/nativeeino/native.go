package nativeeino

import (
	"context"
	"encoding/json"
	"errors"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/observe"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

type business interface {
	Observe(context.Context, protocol.Input) (string, error)
	Apply(context.Context, protocol.Input) (protocol.Receipt, error)
	Inspect(context.Context, protocol.Input) (protocol.Evidence, error)
}
type checkpoint interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}
type storage struct {
	ref   forest.Ref[checkpoint]
	meter *observe.Meter
}

func (s storage) Get(c context.Context, k string) (b []byte, ok bool, e error) {
	e = ef.Guard(c, func() error {
		if e := s.meter.At(c, "checkpoint.get", "", protocol.Input{}, 0, nil); e != nil {
			return e
		}
		return s.ref.Use(c, func(p checkpoint) error { b, ok, e = p.Get(c, k); return e })
	})
	return
}
func (s storage) Set(c context.Context, k string, b []byte) error {
	return ef.Guard(c, func() error {
		if e := s.meter.At(c, "checkpoint.set", "", protocol.Input{}, 0, nil); e != nil {
			return e
		}
		return s.ref.Use(c, func(p checkpoint) error { return p.Set(c, k, b) })
	})
}

type offline struct {
	m *observe.Meter
	h *observe.Handle
}

func (m offline) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) { return m, nil }
func (m offline) Generate(ctx context.Context, msg []*schema.Message, _ ...model.Option) (out *schema.Message, e error) {
	e = ef.Guard(ctx, func() error {
		m.m.Model.Add(1)
		if e := m.h.Check(); e != nil {
			return e
		}
		if len(msg) == 0 {
			return protocol.ErrConflict
		}
		var in protocol.Input
		for _, v := range msg {
			if v.Role == schema.User {
				if json.Unmarshal([]byte(v.Content), &in) != nil {
					return protocol.ErrConflict
				}
				break
			}
		}
		if e := m.m.At(ctx, "model", "", in, 0, nil); e != nil {
			return e
		}
		last := msg[len(msg)-1]
		if last.Role == schema.Tool && last.ToolCallID == "await" {
			out = schema.AssistantMessage(last.Content, nil)
			return nil
		}
		name, id := "apply_once", "apply"
		if last.Role == schema.Tool {
			name, id = "await_continue", "await"
		}
		b, _ := json.Marshal(in)
		out = schema.AssistantMessage("", []schema.ToolCall{{ID: id, Function: schema.FunctionCall{Name: name, Arguments: string(b)}}})
		return nil
	})
	return
}
func (m offline) Stream(ctx context.Context, msg []*schema.Message, o ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	out, e := m.Generate(ctx, msg, o...)
	if e != nil {
		return nil, e
	}
	return schema.StreamReaderFromArray([]*schema.Message{out}), nil
}

type nativeTool struct {
	name string
	ref  forest.Ref[business]
	m    *observe.Meter
	h    *observe.Handle
}

func (t nativeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{Name: t.name, Desc: "deterministic artifact workflow", ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{"ExecutionID": {Type: schema.String, Required: true}, "OperationID": {Type: schema.String, Required: true}, "Text": {Type: schema.String, Required: true}})}, nil
}
func (t nativeTool) InvokableRun(ctx context.Context, args string, _ ...tool.Option) (out string, err error) {
	err = ef.Guard(ctx, func() error {
		if e := t.h.Check(); e != nil {
			return e
		}
		var in protocol.Input
		if len(args) > 4096 || json.Unmarshal([]byte(args), &in) != nil || !protocol.Valid(in) {
			return protocol.ErrConflict
		}
		if t.name == "await_continue" {
			was, has, saved := tool.GetInterruptState[string](ctx)
			if !was {
				return tool.StatefulInterrupt(ctx, "continue or stop", args)
			}
			if !has || saved != args {
				return protocol.ErrConflict
			}
			target, data, value := tool.GetResumeContext[string](ctx)
			if !target || !data {
				return tool.StatefulInterrupt(ctx, "continue or stop", saved)
			}
			if value == "stop" {
				out = "stopped"
				return nil
			}
			if value != "continue" {
				return protocol.ErrConflict
			}
			return t.ref.Use(ctx, func(b business) error {
				o, e := b.Inspect(ctx, in)
				if e != nil {
					return e
				}
				if o.Artifacts != 1 {
					return protocol.ErrUnknown
				}
				out = "finished:" + o.Receipt.Content
				return nil
			})
		}
		if e := t.ref.Use(ctx, func(b business) error { _, e := b.Observe(ctx, in); return e }); e != nil {
			return e
		}
		if e := t.m.At(ctx, "tool.between", "", in, 0, nil); e != nil {
			return e
		}
		return t.ref.Use(ctx, func(b business) error {
			r, e := b.Apply(ctx, in)
			if e == nil {
				data, _ := json.Marshal(r)
				out = string(data)
			}
			return e
		})
	})
	return
}
func ADK(owner *ef.Owner, m *observe.Meter, directory string, h *observe.Handle) (forest.Definition, error) {
	return ef.ADKDefinition(ef.ADKOptions{Config: ef.Config{TypeID: "te.adk", Version: "1", Namespace: "adk", Owner: owner, Ports: []forest.PortDescriptor{forest.Port[business]("business", forest.Required()), forest.Port[checkpoint]("state", forest.Required(), forest.During(forest.Construction))}}, Build: func(_ context.Context, e *forest.BuildEnv) (*ef.ADKHost, error) {
		b, err := forest.Reference[business](e, "business")
		if err != nil {
			return nil, err
		}
		s, err := forest.Reference[checkpoint](e, "state")
		if err != nil {
			return nil, err
		}
		return &ef.ADKHost{Lifecycle: forest.Hooks{OnStart: func(context.Context) error { return h.Open(directory) }, OnStop: func(context.Context) error { return h.Close() }}, NewSession: func(c context.Context) (ef.ADKSession, error) {
			a, err := adk.NewChatModelAgent(c, &adk.ChatModelAgentConfig{Name: "artifact", Description: "offline artifact agent", Model: offline{m, h}, ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{nativeTool{"apply_once", b, m, h}, nativeTool{"await_continue", b, m, h}}}}, MaxIterations: 6})
			if err != nil {
				return ef.ADKSession{}, err
			}
			return ef.ADKSession{Runner: adk.NewRunner(c, adk.RunnerConfig{Agent: a, EnableStreaming: true, CheckPointStore: storage{s, m}}), Lifetime: ef.ExecutionFrom(c)}, nil
		}}, nil
	}})
}
func Graph(owner *ef.Owner, m *observe.Meter) (forest.Definition, error) {
	return ef.GraphDefinition(ef.GraphOptions[string, string]{Config: ef.Config{TypeID: "te.graph", Version: "1", Namespace: "graph", Owner: owner, Ports: []forest.PortDescriptor{forest.Port[business]("business", forest.Required())}}, Build: func(ctx context.Context, e *forest.BuildEnv) (*ef.GraphHost[string, string], error) {
		r, err := forest.Reference[business](e, "business")
		if err != nil {
			return nil, err
		}
		g := compose.NewGraph[string, string]()
		invoke := func(c context.Context, s string, _ ...struct{}) (out string, e error) {
			e = ef.Guard(c, func() error {
				m.Graph.Add(1)
				var in protocol.Input
				if json.Unmarshal([]byte(s), &in) != nil {
					return protocol.ErrConflict
				}
				e = r.Use(c, func(b business) error { out, e = b.Observe(c, in); return e })
				if e != nil {
					return e
				}
				if e = m.At(c, "graph.between", "", in, 0, nil); e != nil {
					return e
				}
				return r.Use(c, func(b business) error { v, e := b.Observe(c, in); out += "/" + v; return e })
			})
			return
		}
		stream := func(c context.Context, raw string, _ ...struct{}) (*schema.StreamReader[string], error) {
			var in protocol.Input
			if json.Unmarshal([]byte(raw), &in) != nil || !protocol.Valid(in) {
				return nil, protocol.ErrConflict
			}
			reader, writer := schema.Pipe[string](0)
			err := ef.Go(c, func(c context.Context) error {
				defer writer.Close()
				m.Graph.Add(1)
				var first string
				if e := r.Use(c, func(b business) error { var e error; first, e = b.Observe(c, in); return e }); e != nil {
					writer.Send("", e)
					return e
				}
				if writer.Send(first+"/", nil) {
					return nil
				}
				if e := m.At(c, "stream.producer", "", in, 0, nil); e != nil {
					writer.Send("", e)
					return e
				}
				var last string
				if e := r.Use(c, func(b business) error { var e error; last, e = b.Observe(c, in); return e }); e != nil {
					writer.Send("", e)
					return e
				}
				writer.Send(last, nil)
				return nil
			})
			if err != nil {
				reader.Close()
				writer.Close()
				return nil, err
			}
			return reader, nil
		}
		lambda, err := compose.AnyLambda(invoke, stream, nil, nil)
		if err != nil {
			return nil, err
		}
		err = g.AddLambdaNode("read", lambda)
		if err != nil {
			return nil, err
		}
		if err = g.AddEdge(compose.START, "read"); err != nil {
			return nil, err
		}
		if err = g.AddEdge("read", compose.END); err != nil {
			return nil, err
		}
		compiled, err := g.Compile(ctx)
		if err != nil {
			return nil, err
		}
		return &ef.GraphHost[string, string]{Lifecycle: forest.Hooks{}, NewSession: func(c context.Context) (ef.GraphSession[string, string], error) {
			if ef.ExecutionFrom(c) == nil {
				return ef.GraphSession[string, string]{}, errors.New("missing owned execution")
			}
			return ef.GraphSession[string, string]{Runnable: compiled, Lifetime: ef.ExecutionFrom(c)}, nil
		}}, nil
	}})
}
