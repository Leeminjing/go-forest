package steps

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	ef "github.com/Leeminjing/go-forest/adapters/eino"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/observe"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

type agent interface {
	Query(context.Context, string, ef.RunOptions, ef.Visitor) (ef.Result, error)
	Resume(context.Context, *adk.ResumeParams, ef.RunOptions, ef.Visitor) (ef.Result, error)
}
type persistence interface {
	Begin(context.Context, protocol.Request) (*protocol.Result, func(), error)
	Seal(context.Context, protocol.Request, protocol.Result) error
	CheckpointDigest(context.Context, string) (string, error)
	Inspect(context.Context, protocol.Input) (protocol.Evidence, error)
}
type Service struct {
	agent forest.Ref[agent]
	state forest.Ref[persistence]
	m     *observe.Meter
}

func Definition(m *observe.Meter) forest.Definition {
	return forest.Definition{TypeID: "te.steps", Version: "1", Ports: []forest.PortDescriptor{forest.Port[agent]("agent", forest.Required()), forest.Port[persistence]("state", forest.Required(), forest.During(forest.Construction))}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Service]("steps", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		a, err := forest.Reference[agent](e, "agent")
		if err != nil {
			return nil, err
		}
		s, err := forest.Reference[persistence](e, "state")
		if err != nil {
			return nil, err
		}
		return forest.Hooks{}, forest.Provide(e, "steps", &Service{a, s, m})
	}}
}
func (s *Service) Step(ctx context.Context, in protocol.Request) (out protocol.Result, err error) {
	if !protocol.Valid(in.Input) || !protocol.Key(in.WorkflowID) || !protocol.Key(in.RunID) {
		return out, protocol.ErrConflict
	}
	if err = s.m.At(ctx, "step.admission", "", in.Input, in.Step, nil); err != nil {
		return
	}
	err = s.state.Use(ctx, func(p persistence) error {
		cached, release, e := p.Begin(ctx, in)
		if e != nil {
			return e
		}
		defer release()
		if cached != nil {
			out = *cached
			return nil
		}
		out.Process = s.m.Process
		out.Generation, _ = s.m.Forest.Generation(ctx)
		var targets []string
		visit := func(event *adk.AgentEvent) error {
			if event.Action != nil && event.Action.Interrupted != nil {
				for _, i := range event.Action.Interrupted.InterruptContexts {
					if i != nil && i.ID != "" {
						targets = append(targets, i.ID)
					}
				}
			}
			msg, e := ef.ReadMessage(event)
			if e != nil {
				return e
			}
			if msg != nil && msg.Role == schema.Assistant && msg.Content != "" {
				if len(msg.Content) > 64<<10 {
					return protocol.ErrPartial
				}
				out.Text = msg.Content
			}
			return nil
		}
		var result ef.Result
		e = s.agent.Use(ctx, func(a agent) error {
			var e error
			o := ef.RunOptions{CheckpointID: protocol.Checkpoint(in.Input)}
			if in.Action == "run" {
				s.m.Query.Add(1)
				b, _ := json.Marshal(in.Input)
				result, e = a.Query(ctx, string(b), o, visit)
			} else {
				s.m.Resume.Add(1)
				result, e = a.Resume(ctx, &adk.ResumeParams{Targets: map[string]any{in.Decision.Target: in.Decision.Value}}, o, visit)
			}
			return e
		})
		if e == nil && result.Canceled {
			e = context.Canceled
		}
		if e == nil {
			evidence, x := p.Inspect(ctx, in.Input)
			if x != nil {
				e = x
			} else {
				out.Receipt = evidence.Receipt
			}
		}
		if e == nil && result.Interrupted {
			sort.Strings(targets)
			if len(targets) != 1 {
				e = protocol.ErrUnknown
			} else {
				digest, x := p.CheckpointDigest(ctx, protocol.Checkpoint(in.Input))
				e = x
				out.Pause = protocol.Pause{Checkpoint: protocol.Checkpoint(in.Input), Digest: digest, Targets: targets}
				out.Pause.Token = protocol.Hash(struct {
					Input protocol.Input
					Step  int
					Pause protocol.Pause
				}{in.Input, in.Step, out.Pause})
				out.State = "paused"
			}
		} else if e == nil {
			if out.Text == "" {
				e = protocol.ErrUnknown
			} else if out.Text == "stopped" {
				out.State = "stopped"
			} else {
				out.State = "completed"
			}
		}
		if e != nil {
			out.State = "unknown"
		}
		out.Trace = observe.Trace(ctx)
		finalize, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if x := p.Seal(finalize, in, out); x != nil {
			return errors.Join(protocol.ErrUnknown, x)
		}
		if e != nil {
			return errors.Join(protocol.ErrUnknown, e)
		}
		if x := s.m.At(ctx, "step.sealed", "", in.Input, in.Step, &out); x != nil {
			return x
		}
		return nil
	})
	return
}
