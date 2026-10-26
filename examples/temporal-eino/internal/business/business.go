package business

import (
	"context"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/internal/observe"
	"github.com/Leeminjing/go-forest/examples/temporal-eino/protocol"
	"github.com/Leeminjing/go-forest/forest"
)

type persistence interface {
	Apply(context.Context, protocol.Input, protocol.Receipt) (protocol.Receipt, error)
	Inspect(context.Context, protocol.Input) (protocol.Evidence, error)
}
type Service struct {
	ref     forest.Ref[persistence]
	meter   *observe.Meter
	version string
	handle  *observe.Handle
}

func Definition(f *forest.Forest, m *observe.Meter, version, directory string, h *observe.Handle) forest.Definition {
	return forest.Definition{TypeID: "te.business", Version: version, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[persistence]("state", forest.Required(), forest.During(forest.Construction))}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[*Service]("business", forest.Public)}, Build: func(_ context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		r, err := forest.Reference[persistence](e, "state")
		if err != nil {
			return nil, err
		}
		s := &Service{ref: r, meter: m, version: version, handle: h}
		return forest.Hooks{OnStart: func(context.Context) error { return h.Open(directory) }, OnStop: func(context.Context) error { return h.Close() }}, forest.Provide(e, "business", s)
	}}
}
func (s *Service) Observe(ctx context.Context, in protocol.Input) (string, error) {
	if e := s.handle.Check(); e != nil {
		return "", e
	}
	if e := s.meter.At(ctx, "business.observe", s.version, in, 0, nil); e != nil {
		return "", e
	}
	return s.version, nil
}
func (s *Service) Apply(ctx context.Context, in protocol.Input) (out protocol.Receipt, err error) {
	s.meter.Apply.Add(1)
	if !protocol.Valid(in) {
		return out, protocol.ErrConflict
	}
	if err = s.handle.Check(); err != nil {
		return
	}
	if err = s.meter.At(ctx, "business.apply", s.version, in, 0, nil); err != nil {
		return
	}
	g, err := s.meter.Forest.Generation(ctx)
	if err != nil {
		return out, err
	}
	out = protocol.Receipt{OperationID: in.OperationID, Digest: protocol.Hash(struct{ Schema, Text string }{"artifact/v1", in.Text}), Content: s.version + ":" + in.Text, Version: s.version, Process: s.meter.Process, Generation: g}
	err = s.ref.Use(ctx, func(p persistence) error { var e error; out, e = p.Apply(ctx, in, out); return e })
	if err != nil {
		return out, err
	}
	if err = s.meter.At(ctx, "effect.published", s.version, in, 0, nil); err != nil {
		return out, protocol.ErrUnknown
	}
	return
}
func (s *Service) Inspect(ctx context.Context, in protocol.Input) (out protocol.Evidence, err error) {
	if err = s.handle.Check(); err != nil {
		return
	}
	if err = s.meter.At(ctx, "business.inspect", s.version, in, 0, nil); err != nil {
		return
	}
	err = s.ref.Use(ctx, func(p persistence) error { var e error; out, e = p.Inspect(ctx, in); return e })
	return
}
