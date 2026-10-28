// Package incrementaldemo demonstrates a host-controlled Gin entry point over
// an incrementally managed Forest. No adapter flags or native behavior change.
package incrementaldemo

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ginforest "github.com/Leeminjing/go-forest/adapters/gin"
	gormforest "github.com/Leeminjing/go-forest/adapters/gorm"
	"github.com/Leeminjing/go-forest/extensions/topology"
	"github.com/Leeminjing/go-forest/forest"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Both interfaces belong to their consumers, not the infrastructure modules.
type database interface {
	WithDB(context.Context, func(*gorm.DB) error) error
	Transaction(context.Context, func(*gorm.DB) error, ...*sql.TxOptions) error
}
type business interface {
	Execute(context.Context, string, bool) (Result, error)
}
type Result struct {
	Versions    []string
	Generations []uint64
	Values      []int
	Pools       []uintptr
}
type Counts struct{ Builds, Starts, Stops int32 }
type tally struct{ builds, starts, stops atomic.Int32 }

func (p *tally) counts() Counts { return Counts{p.builds.Load(), p.starts.Load(), p.stops.Load()} }

type observed struct {
	forest.Lifecycle
	probe *tally
}

func (o *observed) Start(ctx context.Context) error {
	o.probe.starts.Add(1)
	return o.Lifecycle.Start(ctx)
}
func (o *observed) Ready(ctx context.Context) error {
	if r, ok := o.Lifecycle.(forest.Readiness); ok {
		return r.Ready(ctx)
	}
	return nil
}
func (o *observed) Stop(ctx context.Context) error {
	err := o.Lifecycle.Stop(ctx)
	o.probe.stops.Add(1)
	return err
}
func observe(d forest.Definition, p *tally) forest.Definition {
	build := d.Build
	d.Build = func(ctx context.Context, e *forest.BuildEnv) (forest.Lifecycle, error) {
		p.builds.Add(1)
		v, err := build(ctx, e)
		if v == nil {
			return nil, err
		}
		return &observed{v, p}, err
	}
	return d
}

type service struct {
	f                *forest.Forest
	db               forest.Ref[database]
	version          string
	entered, release chan struct{}
}

func (s *service) Execute(ctx context.Context, key string, hold bool) (Result, error) {
	out := Result{}
	for i := 0; i < 2; i++ {
		g, err := s.f.Generation(ctx)
		if err != nil {
			return out, err
		}
		var value int
		var pool uintptr
		err = s.db.Use(ctx, func(d database) error {
			return d.WithDB(ctx, func(db *gorm.DB) error {
				p, e := db.DB()
				if e != nil {
					return e
				}
				pool = reflect.ValueOf(p).Pointer()
				return db.Raw("SELECT 41").Scan(&value).Error
			})
		})
		if err != nil {
			return out, err
		}
		out.Versions = append(out.Versions, s.version)
		out.Generations = append(out.Generations, g)
		out.Values = append(out.Values, value)
		out.Pools = append(out.Pools, pool)
		if i == 0 && hold {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
				return out, ctx.Err()
			}
		}
	}
	err := s.db.Use(ctx, func(d database) error {
		return d.Transaction(ctx, func(db *gorm.DB) error {
			return db.Exec("INSERT INTO forest_topology_receipts (id, version) VALUES (?, ?) ON CONFLICT (id) DO NOTHING", key, s.version).Error
		})
	})
	return out, err
}

type Evidence struct {
	Old, New, Rollback                                    Result
	Before, After, Reverted                               map[forest.TreeID]string
	Counts                                                map[string]Counts
	EndpointBefore, EndpointAfter, EndpointRollback       uintptr
	Engine                                                uintptr
	Pool                                                  uintptr
	ClosedBeforeOpen, ClosedDuringPrepare                 int
	BusinessCallsBeforeBlocked, BusinessCallsAfterBlocked int32
	ReceiptVersion                                        string
	PoolClosed                                            bool
}

// Run uses a real PostgreSQL service, real TCP HTTP, real adapter lifecycles and
// controlled callback barriers. directory must be fresh, local and trusted.
func Run(ctx context.Context, dsn, directory string) (ev Evidence, err error) {
	f := forest.New(forest.Options{})
	var controller atomic.Pointer[topology.Controller]
	oldEntered, oldRelease := make(chan struct{}), make(chan struct{})
	candidateEntered, candidateRelease := make(chan struct{}), make(chan struct{})
	var oldOnce, candidateOnce sync.Once
	defer func() { oldOnce.Do(func() { close(oldRelease) }); candidateOnce.Do(func() { close(candidateRelease) }) }()
	probes := map[string]*tally{"gin": {}, "db": {}, "business-1": {}, "business-2": {}}
	var calls atomic.Int32
	var pool *sql.DB
	var keyBytes [12]byte
	if _, err = rand.Read(keyBytes[:]); err != nil {
		return ev, err
	}
	key := hex.EncodeToString(keyBytes[:])
	defs := []forest.Definition{}
	dbDef, e := gormforest.Definition(f, gormforest.Options{TypeID: "demo.database", Version: "1", Ownership: gormforest.Owned, Open: func(ctx context.Context) (*gorm.DB, error) {
		db, e := gorm.Open(postgres.Open(dsn), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
		if e == nil {
			pool, e = db.DB()
		}
		return db, e
	}})
	if e != nil {
		return ev, e
	}
	defs = append(defs, observe(dbDef, probes["db"]))
	for _, v := range []string{"1", "2"} {
		v := v
		d := forest.Definition{TypeID: "demo.business", Version: v, HotSwappable: true, Ports: []forest.PortDescriptor{forest.Port[database]("db", forest.Required())}, Capabilities: []forest.CapabilityDescriptor{forest.Capability[business]("execute", forest.Public)}, Build: func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle, error) {
			ref, e := forest.Reference[database](env, "db")
			if e != nil {
				return nil, e
			}
			svc := &service{f, ref, v, oldEntered, oldRelease}
			return forest.Hooks{OnStart: func(startCtx context.Context) error {
				if v == "2" {
					close(candidateEntered)
					select {
					case <-candidateRelease:
					case <-startCtx.Done():
						return startCtx.Err()
					}
				}
				return nil
			}}, forest.Provide(env, "execute", svc)
		}}
		defs = append(defs, observe(d, probes["business-"+v]))
	}
	ginDef, e := ginforest.Definition(f, ginforest.Options{TypeID: "demo.gin", Version: "1", Ports: []forest.PortDescriptor{forest.Port[business]("business", forest.Required())}, BuildRouter: func(_ context.Context, env *forest.BuildEnv) (*gin.Engine, error) {
		ref, e := forest.Reference[business](env, "business")
		if e != nil {
			return nil, e
		}
		r := gin.New()
		ev.Engine = reflect.ValueOf(r).Pointer()
		// This MUST precede native Scope: entering f first would bypass the
		// controller's gate via its legitimate nested-Scope continuation rule.
		r.Use(func(c *gin.Context) {
			ctl := controller.Load()
			if ctl == nil {
				c.AbortWithStatus(503)
				return
			}
			e := ctl.Invoke(c.Request.Context(), func(call context.Context) error { c.Request = c.Request.WithContext(call); c.Next(); return nil })
			if e != nil {
				c.AbortWithStatus(503)
			}
		})
		r.Use(ginforest.Scope(f))
		r.GET("/execute", func(c *gin.Context) {
			calls.Add(1)
			var result Result
			e := ref.Use(c.Request.Context(), func(b business) error {
				var e error
				result, e = b.Execute(c.Request.Context(), c.Query("key"), c.Query("hold") == "true")
				return e
			})
			if e != nil {
				c.JSON(500, gin.H{"error": "execution failed"})
				return
			}
			c.JSON(200, result)
		})
		return r, nil
	}})
	if e != nil {
		return ev, e
	}
	defs = append(defs, observe(ginDef, probes["gin"]))
	regs := []topology.Registration{}
	for _, d := range defs {
		regs = append(regs, topology.Registration{Definition: d, ContractKey: d.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	catalog, e := topology.NewCatalog(regs)
	if e != nil {
		return ev, e
	}
	m := topology.Empty()
	for _, spec := range []struct {
		id  forest.TreeID
		ref forest.DefinitionRef
	}{{"gin", ginDef.Ref()}, {"business", defs[1].Ref()}, {"db", dbDef.Ref()}} {
		m.Trees = append(m.Trees, topology.Tree{ID: spec.id, Definition: spec.ref, ContractKey: spec.ref.TypeID, ImplementationDigest: strings.Repeat("a", 64)})
	}
	m.Bindings = []topology.Binding{{ID: "http-business", Consumer: "gin", Port: "business", Provider: "business", Capability: "execute", Kind: "link", Ordinal: 1}, {ID: "business-db", Consumer: "business", Port: "db", Provider: "db", Capability: "database", Kind: "link", Ordinal: 2}}
	store, e := topology.Open(directory, topology.Options{})
	if e != nil {
		return ev, e
	}
	defer func() {
		if controller.Load() == nil {
			_ = store.Close()
		}
	}()
	seed, e := store.Seed(ctx, topology.Request{ID: "seed", Expected: store.Inspect().Position, Approval: "host owns fresh resources"}, m)
	if e != nil {
		return ev, e
	}
	c, e := topology.NewController(f, store, catalog)
	if e != nil {
		return ev, e
	}
	controller.Store(c)
	defer func() {
		oldOnce.Do(func() { close(oldRelease) })
		candidateOnce.Do(func() { close(candidateRelease) })
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, c.Close(closeCtx))
	}()
	if _, e = c.Restore(ctx, topology.Request{ID: "restore", Expected: store.Inspect().Position, Approval: "explicit cold startup"}); e != nil {
		return ev, e
	}
	snapshotIDs := func() map[forest.TreeID]string {
		out := map[forest.TreeID]string{}
		for id, t := range f.Snapshot().Trees {
			out[id] = t.InstanceID
		}
		return out
	}
	endpoint := func() (string, uintptr, error) {
		var url string
		var id uintptr
		e := f.Invoke(ctx, func(call context.Context) error {
			return forest.Use[ginforest.Endpoint](call, f, "gin", "http", func(e ginforest.Endpoint) error {
				url = e.URL()
				id = reflect.ValueOf(e).Pointer()
				return e.ServeError()
			})
		})
		return url, id, e
	}
	url, id, e := endpoint()
	if e != nil {
		return ev, e
	}
	ev.EndpointBefore = id
	ev.Before = snapshotIDs()
	ev.Pool = reflect.ValueOf(pool).Pointer()
	client := &http.Client{Timeout: 20 * time.Second}
	request := func(suffix string) (Result, int, error) {
		var out Result
		req, e := http.NewRequestWithContext(ctx, "GET", url+"/execute?key="+key+suffix, nil)
		if e != nil {
			return out, 0, e
		}
		r, e := client.Do(req)
		if e != nil {
			return out, 0, e
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return out, r.StatusCode, nil
		}
		e = json.NewDecoder(r.Body).Decode(&out)
		return out, r.StatusCode, e
	}
	_, ev.ClosedBeforeOpen, e = request("-closed")
	if e != nil {
		return ev, e
	}
	e = f.Invoke(ctx, func(call context.Context) error {
		return forest.Use[database](call, f, "db", "database", func(d database) error {
			return d.WithDB(call, func(db *gorm.DB) error {
				return db.Exec("CREATE TABLE IF NOT EXISTS forest_topology_receipts (id TEXT PRIMARY KEY, version TEXT NOT NULL)").Error
			})
		})
	})
	if e != nil {
		return ev, e
	}
	if e = c.OpenAdmission(ctx); e != nil {
		return ev, e
	}
	type response struct {
		value  Result
		status int
		err    error
	}
	oldDone := make(chan response, 1)
	go func() { r, status, e := request("-old&hold=true"); oldDone <- response{r, status, e} }()
	select {
	case <-oldEntered:
	case <-ctx.Done():
		return ev, ctx.Err()
	}
	target := store.Inspect().Desired.Manifest
	for i := range target.Trees {
		if target.Trees[i].ID == "business" {
			target.Trees[i].Definition.Version = "2"
		}
	}
	proposal, e := c.Preview(target)
	if e != nil {
		return ev, e
	}
	if !reflect.DeepEqual(proposal.RebuildTrees, []forest.TreeID{"business"}) {
		return ev, fmt.Errorf("unexpected rebuild: %v", proposal.RebuildTrees)
	}
	changed := make(chan error, 1)
	go func() {
		_, e := c.Reconfigure(ctx, topology.Request{ID: "upgrade", Expected: proposal.Expected, Approval: "replace only business"}, proposal)
		changed <- e
	}()
	select {
	case <-candidateEntered:
	case <-ctx.Done():
		return ev, ctx.Err()
	}
	ev.BusinessCallsBeforeBlocked = calls.Load()
	_, ev.ClosedDuringPrepare, e = request("-blocked")
	ev.BusinessCallsAfterBlocked = calls.Load()
	if e != nil {
		return ev, e
	}
	candidateOnce.Do(func() { close(candidateRelease) })
	select {
	case e = <-changed:
		if e != nil {
			return ev, e
		}
	case <-ctx.Done():
		return ev, ctx.Err()
	}
	if probes["business-1"].stops.Load() != 0 {
		return ev, errors.New("old business stopped during active request")
	}
	if e = c.OpenAdmission(ctx); e != nil {
		return ev, e
	}
	var status int
	ev.New, status, e = request("-new")
	if e != nil || status != 200 {
		return ev, errors.Join(e, fmt.Errorf("new status %d", status))
	}
	ev.After = snapshotIDs()
	_, ev.EndpointAfter, e = endpoint()
	if e != nil {
		return ev, e
	}
	oldOnce.Do(func() { close(oldRelease) })
	select {
	case r := <-oldDone:
		if r.err != nil || r.status != 200 {
			return ev, errors.Join(r.err, fmt.Errorf("old status %d", r.status))
		}
		ev.Old = r.value
	case <-ctx.Done():
		return ev, ctx.Err()
	}
	if e = f.Wait(ctx); e != nil {
		return ev, e
	}
	back, e := c.PreviewRollback(seed.Revision)
	if e != nil {
		return ev, e
	}
	if _, e = c.Rollback(ctx, topology.Request{ID: "rollback", Expected: back.Expected, Approval: "new local commit, no business compensation"}, back); e != nil {
		return ev, e
	}
	if e = c.OpenAdmission(ctx); e != nil {
		return ev, e
	}
	ev.Rollback, status, e = request("-back")
	if e != nil || status != 200 {
		return ev, errors.Join(e, fmt.Errorf("rollback status %d", status))
	}
	ev.Reverted = snapshotIDs()
	_, ev.EndpointRollback, e = endpoint()
	if e != nil {
		return ev, e
	}
	e = f.Invoke(ctx, func(call context.Context) error {
		return forest.Use[database](call, f, "db", "database", func(d database) error {
			return d.WithDB(call, func(db *gorm.DB) error {
				return db.Raw("SELECT version FROM forest_topology_receipts WHERE id = ?", key+"-new").Scan(&ev.ReceiptVersion).Error
			})
		})
	})
	if e != nil {
		return ev, e
	}
	if e = c.Close(ctx); e != nil {
		return ev, e
	}
	ev.Counts = map[string]Counts{}
	for name, p := range probes {
		ev.Counts[name] = p.counts()
	}
	ev.PoolClosed = pool.PingContext(ctx) != nil
	return ev, nil
}
