package forest

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

type TreeID string
type PortID string
type CapabilityID string

// Visibility is an explicit set of permitted audiences. Zero is private.
type Visibility uint8

const (
	Public Visibility = 1 << iota
	Descendants
	Ancestors
)

type Phase uint8

const (
	Runtime Phase = iota
	Construction
	Startup
)

type Contract struct {
	ID      string
	Version uint64
}

type PortDescriptor struct {
	ID         PortID
	Type       reflect.Type
	Required   bool
	Many       bool
	Phase      Phase
	ContractID string
	MinVersion uint64
	MaxVersion uint64 // zero means unbounded
}

type PortOption func(*PortDescriptor)

func Required() PortOption          { return func(p *PortDescriptor) { p.Required = true } }
func Many() PortOption              { return func(p *PortDescriptor) { p.Many = true } }
func During(phase Phase) PortOption { return func(p *PortDescriptor) { p.Phase = phase } }
func RequiresContract(id string, min, max uint64) PortOption {
	return func(p *PortDescriptor) { p.ContractID, p.MinVersion, p.MaxVersion = id, min, max }
}
func Port[T any](id PortID, options ...PortOption) PortDescriptor {
	p := PortDescriptor{ID: id, Type: reflect.TypeFor[T]()}
	for _, option := range options {
		option(&p)
	}
	return p
}

type CapabilityDescriptor struct {
	ID         CapabilityID
	Type       reflect.Type
	Visibility Visibility
	Contract   Contract
}

// Capability declares the type that Provide must supply. T may be an interface
// or a concrete implementation; consumers always declare their own interfaces.
func Capability[T any](id CapabilityID, visibility Visibility, contract ...Contract) CapabilityDescriptor {
	c := CapabilityDescriptor{ID: id, Type: reflect.TypeFor[T](), Visibility: visibility}
	if len(contract) > 0 {
		c.Contract = contract[0]
	}
	return c
}

type Factory func(context.Context, *BuildEnv) (Lifecycle, error)
type Lifecycle interface {
	Start(context.Context) error
	Stop(context.Context) error
}
type Readiness interface{ Ready(context.Context) error }

// Hooks adapts ordinary objects to Lifecycle. Nil hooks are no-ops.
type Hooks struct {
	OnStart func(context.Context) error
	OnReady func(context.Context) error
	OnStop  func(context.Context) error
}

func (h Hooks) Start(ctx context.Context) error {
	if h.OnStart != nil {
		return h.OnStart(ctx)
	}
	return nil
}
func (h Hooks) Ready(ctx context.Context) error {
	if h.OnReady != nil {
		return h.OnReady(ctx)
	}
	return nil
}
func (h Hooks) Stop(ctx context.Context) error {
	if h.OnStop != nil {
		return h.OnStop(ctx)
	}
	return nil
}

type Definition struct {
	TypeID       string
	Version      string
	Ports        []PortDescriptor
	Capabilities []CapabilityDescriptor
	Build        Factory
	// These flags are implementation promises, backed by the tree's own tests.
	HotSwappable     bool
	PreserveChildren bool
	Reparentable     bool
	ExportChildren   bool
}

type DefinitionRef struct{ TypeID, Version string }

func (d Definition) Ref() DefinitionRef { return DefinitionRef{d.TypeID, d.Version} }

func cloneDefinition(d Definition) Definition {
	d.Ports = append([]PortDescriptor(nil), d.Ports...)
	d.Capabilities = append([]CapabilityDescriptor(nil), d.Capabilities...)
	return d
}

func validateDefinition(d Definition) error {
	if d.TypeID == "" || d.Version == "" || d.Build == nil {
		return ErrInvalidDefinition
	}
	ports := map[PortID]bool{}
	for _, p := range d.Ports {
		if p.ID == "" || ports[p.ID] || p.Type == nil || p.Type.Kind() != reflect.Interface || p.Phase > Startup || (p.MaxVersion != 0 && p.MaxVersion < p.MinVersion) {
			return fmt.Errorf("%w: port %q must be a unique interface with a valid phase and contract", ErrInvalidDefinition, p.ID)
		}
		ports[p.ID] = true
	}
	caps := map[CapabilityID]bool{}
	for _, c := range d.Capabilities {
		if c.ID == "" || caps[c.ID] || c.Type == nil || c.Visibility & ^(Public|Descendants|Ancestors) != 0 {
			return fmt.Errorf("%w: capability %q", ErrInvalidDefinition, c.ID)
		}
		caps[c.ID] = true
	}
	return nil
}

func portOf(d Definition, id PortID) (PortDescriptor, bool) {
	for _, p := range d.Ports {
		if p.ID == id {
			return p, true
		}
	}
	return PortDescriptor{}, false
}
func capOf(d Definition, id CapabilityID) (CapabilityDescriptor, bool) {
	for _, c := range d.Capabilities {
		if c.ID == id {
			return c, true
		}
	}
	return CapabilityDescriptor{}, false
}
func compatible(valueType, target reflect.Type) bool {
	return valueType != nil && target != nil && valueType.AssignableTo(target)
}

type State string

const (
	Created   State = "created"
	Preparing State = "preparing"
	Ready     State = "ready"
	Active    State = "active"
	Draining  State = "draining"
	Stopped   State = "stopped"
	Failed    State = "failed"
)

type Options struct {
	DrainTimeout  time.Duration
	EventCapacity int
}
