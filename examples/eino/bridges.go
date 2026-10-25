package einodemo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/Leeminjing/go-forest-agent/agent"
	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

// The application owns this narrow, finite-text contract. Native state and
// control protocols are deliberately kept at their respective engine entries.
type answerer interface {
	Answer(context.Context, string) (string, error)
}
type Application interface {
	Ask(context.Context, string) (string, error)
}
type application struct{ engine forest.Ref[answerer] }

func (a application) Ask(ctx context.Context, q string) (out string, err error) {
	err = a.engine.Use(ctx, func(e answerer) error { out, err = e.Answer(ctx, q); return err })
	return
}
func applicationDefinition() forest.Definition {
	return forest.Definition{TypeID: "eino-example.application", Version: "1", Ports: []forest.PortDescriptor{forest.Port[answerer]("engine", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[Application]("application", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[answerer](env, "engine")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(env, "application", application{ref})
	}}
}

type ContinuationRequired struct{ Engine, ID string }

func (e *ContinuationRequired) Error() string {
	return "native engine continuation required: " + e.Engine
}
func (e *ContinuationRequired) NativeReference() (string, string) { return e.Engine, e.ID }
func runID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type einoText struct{ agent forest.Ref[ef.ADK] }

func (e einoText) Answer(ctx context.Context, q string) (answer string, err error) {
	id, err := runID()
	if err != nil {
		return "", err
	}
	err = e.agent.Use(ctx, func(a ef.ADK) error {
		result, err := a.Query(ctx, q, ef.RunOptions{CheckpointID: id}, func(event *adk.AgentEvent) error {
			m, err := ef.ReadMessage(event)
			if m != nil && m.Role == schema.Assistant && m.Content != "" {
				answer = "eino:" + m.Content
			}
			return err
		})
		if err != nil {
			return err
		}
		if result.Interrupted || result.Canceled {
			return &ContinuationRequired{Engine: "eino", ID: id}
		}
		if answer == "" {
			return errors.New("native Eino produced no answer")
		}
		return nil
	})
	return
}
func einoBridgeDefinition() forest.Definition {
	return forest.Definition{TypeID: "eino-example.eino-text", Version: "1", Ports: []forest.PortDescriptor{forest.Port[ef.ADK]("agent", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[answerer]("answer", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[ef.ADK](env, "agent")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(env, "answer", einoText{ref})
	}}
}

type forestText struct{ executor forest.Ref[agent.Executor] }

func (e forestText) Answer(ctx context.Context, q string) (answer string, err error) {
	id, err := runID()
	if err != nil {
		return "", err
	}
	err = e.executor.Use(ctx, func(e agent.Executor) error {
		run, err := e.Create(ctx, id, q)
		if err != nil {
			return err
		}
		for i := 0; i < 16; i++ {
			switch run.Status {
			case agent.Completed:
				answer = run.Messages[len(run.Messages)-1].Content
				return nil
			case agent.AwaitingApproval, agent.OutcomeUnknown:
				return &ContinuationRequired{Engine: "forest-agent", ID: id}
			case agent.Failed, agent.Canceled:
				return errors.New("Forest Agent did not complete")
			}
			run, err = e.Advance(ctx, id)
			if err != nil {
				return err
			}
		}
		return &ContinuationRequired{Engine: "forest-agent", ID: id}
	})
	return
}
func forestBridgeDefinition() forest.Definition {
	return forest.Definition{TypeID: "eino-example.forest-text", Version: "1", Ports: []forest.PortDescriptor{forest.Port[agent.Executor]("executor", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[answerer]("answer", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[agent.Executor](env, "executor")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(env, "answer", forestText{ref})
	}}
}

type forestModel struct{}

func (forestModel) Generate(ctx context.Context, r agent.ModelRequest, emit func(string) error) (agent.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return agent.ModelResult{}, err
	}
	if len(r.Messages) == 0 {
		return agent.ModelResult{}, errors.New("missing input")
	}
	last := r.Messages[len(r.Messages)-1]
	if last.Role == "tool" {
		out := "forest:" + last.Content
		if err := emit(out); err != nil {
			return agent.ModelResult{}, err
		}
		return agent.ModelResult{Content: out}, nil
	}
	args, _ := json.Marshal(input{Key: last.Content})
	return agent.ModelResult{Calls: []agent.ToolCall{{ID: "lookup-1", Name: "lookup", Arguments: args}}}, nil
}
func forestModelDefinition() forest.Definition {
	return forest.Definition{TypeID: "eino-example.forest-model", Version: "1", Capabilities: []forest.CapabilityDescriptor{forest.Capability[agent.Model]("model", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		return forest.Hooks{}, forest.Provide(env, "model", forestModel{})
	}}
}

// This tool consumes its own Lookup abstraction and knows neither provider.
type forestLookup interface {
	Lookup(context.Context, string) (string, error)
}
type forestTool struct {
	ref      forest.Ref[forestLookup]
	approval bool
}

func (t forestTool) Describe(context.Context) (agent.ToolSpec, error) {
	return agent.ToolSpec{Name: "lookup", Version: "1", Description: "read shared catalog twice", Parameters: json.RawMessage(`{"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}`), NeedsApproval: t.approval}, nil
}
func (t forestTool) Execute(ctx context.Context, r agent.ToolRequest) (out string, err error) {
	var in input
	if json.Unmarshal(r.Call.Arguments, &in) != nil || in.Key == "" {
		return "", errors.New("invalid lookup arguments")
	}
	err = t.ref.Use(ctx, func(p forestLookup) error {
		one, err := p.Lookup(ctx, in.Key)
		if err != nil {
			return err
		}
		two, err := p.Lookup(ctx, in.Key)
		out = one + "/" + two
		return err
	})
	return
}
func forestToolDefinition(approval bool) forest.Definition {
	return forest.Definition{TypeID: "eino-example.forest-tool", Version: "1", Ports: []forest.PortDescriptor{forest.Port[forestLookup]("lookup", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[agent.Tool]("tool", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
		ref, err := forest.Reference[forestLookup](env, "lookup")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(env, "tool", forestTool{ref, approval})
	}}
}
