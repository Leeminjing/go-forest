"""Exercise released topology APIs in an empty module, without local replacements."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = {
    "github.com/Leeminjing/go-forest": "v0.3.0",
    "github.com/Leeminjing/go-forest/extensions/topology": "v0.1.0",
}

CONSUMER = r'''package main
import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "os"
    topology "github.com/Leeminjing/go-forest/extensions/topology"
    "github.com/Leeminjing/go-forest/forest"
)
type number interface { Number() int }
type implementation int
func (v implementation) Number() int { return int(v) }
func must(err error) { if err != nil { panic(err) } }
func main() {
    ctx := context.Background()
    var entries []topology.Registration
    for _, version := range []string{"1", "2"} {
        version := version
        sum := sha256.Sum256([]byte("external-number/" + version))
        def := forest.Definition{
            TypeID:"external-number", Version:version, HotSwappable:true,
            Capabilities:[]forest.CapabilityDescriptor{forest.Capability[number]("number", forest.Public)},
            Build:func(_ context.Context, env *forest.BuildEnv) (forest.Lifecycle,error) {
                value := implementation(1); if version == "2" { value = 2 }
                return forest.Hooks{}, forest.Provide(env,"number",value)
            },
        }
        entries = append(entries, topology.Registration{Definition:def, ContractKey:"external-number/1", ImplementationDigest:hex.EncodeToString(sum[:])})
    }
    catalog, err := topology.NewCatalog(entries); must(err)
    source := forest.New(forest.Options{})
    for _, entry := range entries { must(source.Register(entry.Definition)) }
    _,err=source.Apply(ctx,forest.ChangePlan{Operations:[]forest.Operation{forest.AddRoot("number",entries[0].Definition.Ref())}}); must(err)
    manifest,err:=topology.Capture(source.Snapshot(),catalog); must(err); must(source.Close(ctx))
    dir,err:=os.MkdirTemp("","external-topology-"); must(err); defer os.RemoveAll(dir)
    store,err:=topology.Open(dir,topology.Options{}); must(err)
    seed,err:=store.Seed(ctx,topology.Request{ID:"seed",Expected:store.Inspect().Position,Approval:"external captured desired structure"},manifest);must(err)
    f:=forest.New(forest.Options{}); controller,err:=topology.NewController(f,store,catalog);must(err)
    first,err:=controller.Restore(ctx,topology.Request{ID:"boot",Expected:store.Inspect().Position,Approval:"source exited, recreate resources"});must(err)
    values:=[]int{}
    call:=func(){must(controller.OpenAdmission(ctx));must(controller.Invoke(ctx,func(call context.Context)error{return forest.Use[number](call,f,"number","number",func(v number)error{values=append(values,v.Number());return nil})}))}
    call()
    data,err:=topology.Encode(manifest);must(err);target,err:=topology.Decode(data);must(err)
    target.Trees[0].Definition=entries[1].Definition.Ref();target.Trees[0].ImplementationDigest=entries[1].ImplementationDigest
    proposal,err:=controller.Preview(target);must(err)
    second,err:=controller.Reconfigure(ctx,topology.Request{ID:"change",Expected:proposal.Expected,Approval:"full rebuild"},proposal);must(err);call()
    proposal,err=controller.PreviewRollback(seed.Revision);must(err)
    third,err:=controller.Rollback(ctx,topology.Request{ID:"rollback",Expected:proposal.Expected,Approval:"topology rollback only"},proposal);must(err);call();must(controller.Close(ctx))
    store,err=topology.Open(dir,topology.Options{});must(err);f=forest.New(forest.Options{});controller,err=topology.NewController(f,store,catalog);must(err)
    cold,err:=controller.Restore(ctx,topology.Request{ID:"second-boot",Expected:store.Inspect().Position,Approval:"old controller exited, allow new objects"});must(err);call();must(controller.Close(ctx))
    result:=struct{Values []int;Generations []uint64;ColdGeneration uint64;NewSession bool}{values,[]uint64{first.Generation,second.Generation,third.Generation},cold.Generation,cold.SessionID!=first.SessionID}
    encoded,err:=json.Marshal(result);must(err);fmt.Println(string(encoded))
}
'''


def verify(versions, consumer, expected, label="Capture/Seed/Restore/Invoke/Reconfigure/Rollback/new session"):
    with tempfile.TemporaryDirectory(prefix="forest-topology-consumer-") as temporary:
        directory = Path(temporary)
        env = dict(os.environ, GOWORK="off", GOMODCACHE=str(directory / "module-cache"))

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/topology-consumer")
        (directory / "main.go").write_text(consumer, encoding="utf-8")
        run("gofmt", "-w", "main.go")
        run("go", "get", *(path + "@" + version for path, version in versions.items()))
        run("go", "mod", "tidy")
        run("go", "mod", "verify")
        payload = run("go", "list", "-m", "-json", "all", capture=True)
        decoder, modules = json.JSONDecoder(), {}
        while payload.strip():
            payload = payload.lstrip()
            item, count = decoder.raw_decode(payload)
            payload = payload[count:]
            assert "Replace" not in item, "published dependency has replace"
            modules[item["Path"]] = item
        for path, version in versions.items():
            item = modules[path]
            assert item["Version"] == version, item
            assert not Path(item["Dir"]).is_relative_to(ROOT), item
            print(path + "@" + version + ": fixed external download", flush=True)
        run("go", "vet", "./...")
        evidence = json.loads(run("go", "run", ".", capture=True))
        assert evidence == expected, evidence
        print(json.dumps(evidence), flush=True)
        print("PASS: " + label, flush=True)


def main():
    verify(VERSIONS, CONSUMER, {"Values": [1, 2, 1, 1], "Generations": [1, 2, 3],
                                "ColdGeneration": 1, "NewSession": True})


if __name__ == "__main__":
    main()
