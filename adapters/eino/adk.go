package einoforest

import (
	"context"
	"errors"
	"io"

	"github.com/Leeminjing/go-forest/forest"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"
)

type RunOptions struct {
	// Raw root checkpoint/cancel options are reserved and overridden. Do not
	// designate conflicting identity options for descendants.
	CheckpointID  string
	Options       []adk.AgentRunOption
	OnCancelReady func(adk.AgentCancelFunc)
}
type Result struct {
	Events                uint64
	Interrupted, Canceled bool
}
type Visitor func(*adk.AgentEvent) error
type ADK interface {
	Run(context.Context, []*schema.Message, RunOptions, Visitor) (Result, error)
	Query(context.Context, string, RunOptions, Visitor) (Result, error)
	Resume(context.Context, *adk.ResumeParams, RunOptions, Visitor) (Result, error)
	Status() Status
}
type ADKSession struct {
	Runner   *adk.Runner
	Lifetime Lifetime
}
type ADKHost struct {
	Lifecycle  forest.Lifecycle
	NewSession func(context.Context) (ADKSession, error)
}
type ADKOptions struct {
	Config    Config
	Build     func(context.Context, *forest.BuildEnv) (*ADKHost, error)
	MaxEvents int
}

func ADKDefinition(o ADKOptions) (forest.Definition, error) {
	if !o.Config.valid() || o.Build == nil || o.MaxEvents < 0 || o.MaxEvents > 65536 {
		return forest.Definition{}, ErrInvalid
	}
	if o.MaxEvents == 0 {
		o.MaxEvents = 4096
	}
	o.Config.Ports = append([]forest.PortDescriptor(nil), o.Config.Ports...)
	return forest.Definition{TypeID: o.Config.TypeID, Version: o.Config.Version, Ports: o.Config.Ports, HotSwappable: o.Config.HotSwappable, Capabilities: []forest.CapabilityDescriptor{forest.Capability[ADK]("agent", forest.Public)}, Build: func(ctx context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		host, err := o.Build(ctx, e)
		if host == nil {
			return nil, errors.Join(err, ErrUnmanaged)
		}
		r := &agent{runtime: runtime{config: o.Config}, host: host, maxEvents: o.MaxEvents}
		if err != nil {
			return r, err
		}
		if host.Lifecycle == nil || host.NewSession == nil {
			return r, ErrUnmanaged
		}
		return r, forest.Provide(e, "agent", r)
	}}, nil
}

type agent struct {
	runtime
	host      *ADKHost
	maxEvents int
}

func (a *agent) Start(ctx context.Context) error { return a.host.Lifecycle.Start(ctx) }
func (a *agent) Ready(ctx context.Context) error {
	if r, ok := a.host.Lifecycle.(forest.Readiness); ok {
		return r.Ready(ctx)
	}
	return nil
}
func (a *agent) Stop(ctx context.Context) error {
	return a.runtime.stop(ctx, a.host.Lifecycle)
}
func (a *agent) execute(ctx context.Context, o RunOptions, visit Visitor, start func(context.Context, *adk.Runner, []adk.AgentRunOption) (*adk.AsyncIterator[*adk.AgentEvent], error)) (result Result, err error) {
	if o.CheckpointID == "" || visit == nil {
		return result, ErrInvalid
	}
	err = a.run(ctx, o.CheckpointID, func(ctx context.Context) (err error) {
		session, err := a.host.NewSession(ctx)
		if session.Lifetime != nil {
			defer func() { err = errors.Join(err, a.join(session.Lifetime)) }()
		}
		if err != nil {
			return err
		}
		if session.Runner == nil || session.Lifetime == nil {
			return ErrUnmanaged
		}
		cancelOption, cancel := adk.WithCancel()
		opts := append([]adk.AgentRunOption(nil), o.Options...)
		opts = append(opts, adk.WithCheckPointID(o.CheckpointID), cancelOption)
		iter, err := start(ctx, session.Runner, opts)
		if err != nil {
			return err
		}
		if iter == nil {
			return ErrInvalid
		}
		var first error
		seen := map[string]bool{}
		record := func(kind string, e error) {
			if e != nil && !seen[kind] {
				seen[kind] = true
				first = errors.Join(first, e)
			}
		}
		if o.OnCancelReady != nil {
			record("callback", safe(func() error { o.OnCancelReady(cancel); return nil }))
			if first != nil {
				session.Lifetime.Cancel()
			}
		}
		for {
			event, ok := iter.Next()
			if !ok {
				break
			}
			result.Events++
			if ctx.Err() != nil {
				record("context", ctx.Err())
				session.Lifetime.Cancel()
			}
			deliver := first == nil
			if event == nil {
				record("invalid", ErrInvalid)
				session.Lifetime.Cancel()
				continue
			}
			if event.Action != nil && event.Action.Interrupted != nil {
				result.Interrupted = true
			}
			if event.Err != nil {
				record("native", event.Err)
				var canceled *adk.CancelError
				if errors.As(event.Err, &canceled) {
					result.Canceled = true
				}
			}
			if result.Events > uint64(a.maxEvents) && !errors.Is(first, ErrBudget) {
				record("budget", ErrBudget)
				session.Lifetime.Cancel()
			}
			var reader *schema.StreamReader[*schema.Message]
			if event.Output != nil && event.Output.MessageOutput != nil && event.Output.MessageOutput.IsStreaming {
				reader = event.Output.MessageOutput.MessageStream
			}
			callbackErr := func() error {
				if reader != nil {
					defer reader.Close()
				}
				if deliver && result.Events <= uint64(a.maxEvents) {
					return safe(func() error { return visit(event) })
				}
				return nil
			}()
			if callbackErr != nil {
				record("callback", callbackErr)
				session.Lifetime.Cancel()
			}
			if ctx.Err() != nil {
				record("context", ctx.Err())
				session.Lifetime.Cancel()
			}
		}
		if ctx.Err() != nil {
			record("context", ctx.Err())
		}
		return first
	})
	return
}
func (a *agent) Run(ctx context.Context, m []*schema.Message, o RunOptions, v Visitor) (Result, error) {
	return a.execute(ctx, o, v, func(ctx context.Context, r *adk.Runner, opts []adk.AgentRunOption) (*adk.AsyncIterator[*adk.AgentEvent], error) {
		return r.Run(ctx, m, opts...), nil
	})
}
func (a *agent) Query(ctx context.Context, q string, o RunOptions, v Visitor) (Result, error) {
	return a.execute(ctx, o, v, func(ctx context.Context, r *adk.Runner, opts []adk.AgentRunOption) (*adk.AsyncIterator[*adk.AgentEvent], error) {
		return r.Query(ctx, q, opts...), nil
	})
}
func (a *agent) Resume(ctx context.Context, p *adk.ResumeParams, o RunOptions, v Visitor) (Result, error) {
	return a.execute(ctx, o, v, func(ctx context.Context, r *adk.Runner, opts []adk.AgentRunOption) (*adk.AsyncIterator[*adk.AgentEvent], error) {
		if p != nil {
			return r.ResumeWithParams(ctx, o.CheckpointID, p, opts...)
		}
		return r.Resume(ctx, o.CheckpointID, opts...)
	})
}

// ReadMessage borrows the stream; the adapter owns Close. Native concatenation
// preserves message fields. Visitors must not call GetMessage, Close or Copy.
func ReadMessage(event *adk.AgentEvent) (*schema.Message, error) {
	if event == nil || event.Output == nil || event.Output.MessageOutput == nil {
		return nil, nil
	}
	m := event.Output.MessageOutput
	if !m.IsStreaming {
		return m.Message, nil
	}
	if m.MessageStream == nil {
		return nil, ErrInvalid
	}
	var chunks []*schema.Message
	var size int
	for {
		chunk, err := m.MessageStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			return nil, ErrInvalid
		}
		size += len(chunk.Content)
		if size > 4<<20 || len(chunks) >= 4096 {
			return nil, ErrBudget
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) == 0 {
		return nil, nil
	}
	return schema.ConcatMessages(chunks)
}
