// planning-instrument edits only an owned source archive, never the checkout.
// Function spans are instrumented separately from ordinary benchmark binaries.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	dir := flag.String("source", "", "owned topology source archive")
	flag.Parse()
	if *dir == "" {
		panic("source required")
	}
	set := token.NewFileSet()
	files := map[string]*ast.File{}
	names := map[string]bool{}
	entries, e := os.ReadDir(*dir)
	must(e)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if strings.HasPrefix(name, "zz_planning_") {
			panic("archive already instrumented")
		}
		f, e := parser.ParseFile(set, filepath.Join(*dir, name), nil, parser.ParseComments)
		must(e)
		files[name] = f
		for _, d := range f.Decls {
			if f, ok := d.(*ast.FuncDecl); ok {
				names[key(f)] = true
			}
		}
	}
	for _, name := range []string{"Normalize", "Capture", "Compare", "Digest", "Catalog.plan", "Controller.previewLocked", "Controller.commit", "hash"} {
		if !names[name] {
			panic("missing expected entry " + name)
		}
	}
	full := "Normalize"
	if names["canonicalize"] {
		full = "canonicalize"
	}
	spans := map[string]string{"Normalize": "Normalize-public", "Capture": "Capture-public", "Encode": "Encode-public", "Digest": "Digest-public", "Compare": "Compare-public", "Controller.previewLocked": "Preview-validation", "Controller.commit": "Reconfigure", "Catalog.plan": "Catalog-plan", "planV2": "Plan-v2", "treeIndex": "tree-index", "bindingIndex": "binding-index", full: "Normalize-full"}
	if names["Catalog.planCanonical"] {
		delete(spans, "Catalog.plan")
		spans["Catalog.planCanonical"] = "Catalog-plan"
	}
	if names["planV2Canonical"] {
		delete(spans, "planV2")
		spans["planV2Canonical"] = "Plan-v2"
	}
	seen := map[string]int{}
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			replacement := ""
			if pkg.Name == "json" && selector.Sel.Name == "Marshal" {
				replacement = "planningMarshal"
			}
			if pkg.Name == "sort" && selector.Sel.Name == "Slice" {
				replacement = "planningSortSlice"
			}
			if pkg.Name == "sort" && selector.Sel.Name == "Strings" {
				replacement = "planningSortStrings"
			}
			if pkg.Name == "reflect" && selector.Sel.Name == "DeepEqual" && len(call.Args) == 2 {
				a, ok := call.Args[0].(*ast.Ident)
				b, ok2 := call.Args[1].(*ast.Ident)
				if ok && ok2 && a.Name == "expected" && b.Name == "proposal" {
					replacement = "planningProposalEqual"
				}
			}
			if replacement != "" {
				call.Fun = ast.NewIdent(replacement)
				seen[replacement]++
			}
			return true
		})
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if label, ok := spans[key(fn)]; ok {
				fn.Body.List = append([]ast.Stmt{statement("defer planningFinish(planningStart(" + strconv.Quote(label) + "))")}, fn.Body.List...)
				seen[label]++
			}
			if key(fn) == "hash" {
				fn.Body.List = append([]ast.Stmt{statement("planningHash(domain, len(data))")}, fn.Body.List...)
			}
		}
		// Replaced selectors may leave an import unused. Keep it only when its
		// package identifier remains in a selector (these files use normal aliases).
		used := map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := s.X.(*ast.Ident); ok {
					used[id.Name] = true
				}
			}
			return true
		})
		var decls []ast.Decl
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
				var specs []ast.Spec
				for _, s := range g.Specs {
					i := s.(*ast.ImportSpec)
					path, _ := strconv.Unquote(i.Path.Value)
					base := filepath.Base(path)
					if (base == "json" || base == "sort" || base == "reflect") && !used[base] {
						continue
					}
					specs = append(specs, s)
				}
				g.Specs = specs
				if len(specs) == 0 {
					continue
				}
			}
			decls = append(decls, d)
		}
		f.Decls = decls
		var out bytes.Buffer
		must(format.Node(&out, set, f))
		must(os.WriteFile(filepath.Join(*dir, name), out.Bytes(), 0600))
	}
	for _, label := range []string{"Normalize-full", "Catalog-plan", "Preview-validation", "Reconfigure", "planningMarshal", "planningProposalEqual"} {
		if seen[label] == 0 {
			panic("instrumentation missing " + label)
		}
	}
	must(os.WriteFile(filepath.Join(*dir, "zz_planning_probe.go"), []byte(helper), 0600))
	fmt.Println("instrumented semantic normalization, phases, JSON, sort, hash and full Proposal comparison")
}
func key(f *ast.FuncDecl) string {
	if f.Recv == nil {
		return f.Name.Name
	}
	t := f.Recv.List[0].Type
	if p, ok := t.(*ast.StarExpr); ok {
		t = p.X
	}
	return t.(*ast.Ident).Name + "." + f.Name.Name
}
func statement(code string) ast.Stmt {
	f, e := parser.ParseFile(token.NewFileSet(), "probe.go", "package topology;func x(){"+code+"}", 0)
	must(e)
	return f.Decls[0].(*ast.FuncDecl).Body.List[0]
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}

const helper = `package topology
import("encoding/json";"reflect";"runtime";"sort";"sync";"time")
type planningSample struct {Calls,Bytes,NS,Alloc,Allocs,SelfNS,SelfAlloc,SelfAllocs uint64}
type planningSpan struct {label string;start time.Time;alloc,allocs,childNS,childAlloc,childAllocs uint64}
var planningState=struct{sync.Mutex;Data map[string]planningSample;Stack []*planningSpan}{Data:map[string]planningSample{}}
func PlanningReset(){planningState.Lock();defer planningState.Unlock();planningState.Data=map[string]planningSample{};planningState.Stack=nil}
func PlanningReport()map[string]planningSample{planningState.Lock();defer planningState.Unlock();out:=map[string]planningSample{};for k,v:=range planningState.Data{out[k]=v};return out}
func planningStart(label string)*planningSpan{var m runtime.MemStats;runtime.ReadMemStats(&m);s:=&planningSpan{label:label,start:time.Now(),alloc:m.TotalAlloc,allocs:m.Mallocs};planningState.Lock();planningState.Stack=append(planningState.Stack,s);planningState.Unlock();return s}
func planningFinish(s *planningSpan){ns:=uint64(time.Since(s.start));var m runtime.MemStats;runtime.ReadMemStats(&m);a,n:=m.TotalAlloc-s.alloc,m.Mallocs-s.allocs;planningState.Lock();defer planningState.Unlock();stack:=planningState.Stack;if len(stack)==0||stack[len(stack)-1]!=s{panic("concurrent instrumentation span; unsupported attribution")};planningState.Stack=stack[:len(stack)-1];v:=planningState.Data[s.label];v.Calls++;v.NS+=ns;v.Alloc+=a;v.Allocs+=n;v.SelfNS+=ns-s.childNS;v.SelfAlloc+=a-s.childAlloc;v.SelfAllocs+=n-s.childAllocs;planningState.Data[s.label]=v;if len(planningState.Stack)>0{p:=planningState.Stack[len(planningState.Stack)-1];p.childNS+=ns;p.childAlloc+=a;p.childAllocs+=n}}
func planningCount(label string,bytes uint64){planningState.Lock();defer planningState.Unlock();v:=planningState.Data[label];v.Calls++;v.Bytes+=bytes;planningState.Data[label]=v}
func planningMarshal(v any)([]byte,error){b,e:=json.Marshal(v);planningCount("json-all",uint64(len(b)));switch v.(type){case Manifest,*Manifest:planningCount("json-manifest",uint64(len(b)))};return b,e}
func planningSortSlice(v any,less func(int,int)bool){planningCount("sort-slice-items",uint64(reflect.ValueOf(v).Len()));sort.Slice(v,less)}
func planningSortStrings(v []string){planningCount("sort-string-items",uint64(len(v)));sort.Strings(v)}
func planningHash(domain string,n int){planningCount("hash-all",uint64(n));if domain=="forest-topology/manifest/v1"{planningCount("hash-manifest",uint64(n))}}
func planningProposalEqual(a,b any)bool{defer planningFinish(planningStart("Proposal-deep-validation"));return reflect.DeepEqual(a,b)}
`
