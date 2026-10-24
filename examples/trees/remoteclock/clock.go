// Package remoteclock provides explicit Clock adapters without importing a
// consumer's interfaces or a particular clock implementation.
package remoteclock

import (
	"context"
	"time"

	bridge "github.com/Leeminjing/go-forest/bridge/httpjson"
	"github.com/Leeminjing/go-forest/forest"
)

const RouteName = "clock.now"
const ContractID = "example.clock.time"

type Source interface {
	Now(context.Context) (time.Time, error)
}
type Reply struct {
	Time time.Time `json:"time"`
}

func Descriptor() bridge.Descriptor {
	return bridge.Descriptor{Name: RouteName, Contract: bridge.Contract{ID: ContractID, Version: 1}}
}
func CapabilityContract() forest.Contract { return forest.Contract{ID: ContractID, Version: 1} }

type Proxy struct{ Client *bridge.Client }

func (p Proxy) Now(ctx context.Context) (time.Time, error) {
	var result Reply
	_, err := p.Client.Call(ctx, RouteName, struct{}{}, &result)
	return result.Time, err
}

// ProxyDefinition advertises exactly the Clock contract implemented by Proxy.
func ProxyDefinition(options bridge.ClientOptions) forest.Definition {
	options.Routes = []bridge.Descriptor{Descriptor()}
	return forest.Definition{TypeID: "example.remote-clock.proxy", Version: "1", HotSwappable: true,
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[Source]("clock", forest.Public, CapabilityContract())},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			client, err := bridge.NewClient(options)
			if err != nil {
				return nil, err
			}
			return client, forest.Provide(env, "clock", Proxy{Client: client})
		}}
}

// HostDefinition consumes a local Clock through its own declared port. Only
// this one route is exported, regardless of the rest of the remote forest.
func HostDefinition(f *forest.Forest, options bridge.HostOptions) forest.Definition {
	return forest.Definition{TypeID: "example.remote-clock.host", Version: "1",
		Ports:        []forest.PortDescriptor{forest.Port[Source]("clock", forest.Required(), forest.RequiresContract(ContractID, 1, 1))},
		Capabilities: []forest.CapabilityDescriptor{forest.Capability[bridge.Endpoint]("endpoint", forest.Public)},
		Build: func(ctx context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, err := forest.Reference[Source](env, "clock")
			if err != nil {
				return nil, err
			}
			local := options
			local.Routes = []bridge.Route{bridge.Handle(RouteName, Descriptor().Contract, func(ctx context.Context, _ struct{}) (Reply, error) {
				var result Reply
				err := ref.Use(ctx, func(clock Source) error { var err error; result.Time, err = clock.Now(ctx); return err })
				return result, err
			})}
			host, err := bridge.NewHost(f, local)
			if err != nil {
				return nil, err
			}
			return host, forest.Provide(env, "endpoint", host)
		}}
}
