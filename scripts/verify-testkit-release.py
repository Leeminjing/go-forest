"""Consume fixed TestKit outside the checkout, with only standard-library fixtures."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/Leeminjing/go-forest/testkit"
VERSION = "v0.1.0"

CONSUMER = r'''package consumer_test
import (
 "context"
 "errors"
 "os"
 "testing"
 "time"
 tk "github.com/Leeminjing/go-forest/testkit"
)
func TestPublishedMechanisms(t *testing.T) {
 ctx,cancel:=context.WithTimeout(context.Background(),time.Second);defer cancel()
 gate:=tk.NewBarrier();defer gate.Release()
 done:=make(chan error,1);go func(){done<-gate.Hold(ctx)}()
 if err:=tk.Await(ctx,gate.Arrived());err!=nil {t.Fatal(err)}
 file,err:=os.CreateTemp(t.TempDir(),"original");if err!=nil {t.Fatal(err)};defer file.Close()
 if _,err=file.Stat();err!=nil {t.Fatal(err)}
 gate.Release();if err=<-done;err!=nil {t.Fatal(err)}
 j,_:=tk.NewJournal(64)
 record:=func(e tk.Event){t.Helper();if _,err:=j.Record(e);err!=nil {t.Fatal(err)}}
 snapshot:=func()tk.Trace {t.Helper();s,err:=j.Snapshot();if err!=nil {t.Fatal(err)};return s}
 scope:=tk.Scope{Forest:"owned",Call:"call",Generation:0}
 record(tk.Event{Stage:"first",Execution:&tk.Execution{Scope:scope}})
 record(tk.Event{Stage:"second",Execution:&tk.Execution{Scope:scope}})
 if err=tk.CheckPinned(tk.PinnedEvidence{Label:"external",Scope:scope,Required:[]string{"first","second"},Trace:snapshot()});err!=nil {t.Fatal(err)}
 work:=tk.Execution{Scope:scope,Task:"task",Step:"step",Attempt:"1",Process:"process",Instance:"instance"}
 before:=tk.Resource{ID:"original",Ownership:tk.Owned,Usable:true}
 if err=file.Close();err!=nil {t.Fatal(err)}
 if _,err=file.Read(make([]byte,1));!errors.Is(err,os.ErrClosed){t.Fatal(err)}
 after:=before;after.Usable=false;after.CloseCount=1
 j,_=tk.NewJournal(64)
 for _,e:=range []tk.Event{{Stage:"entered",Execution:&work,Resources:[]tk.Resource{before}},{Stage:"notified",Execution:&work},{Stage:"held",Execution:&work,Resources:[]tk.Resource{before}},{Stage:"released",Execution:&work},{Stage:"exited",Execution:&work},{Stage:"joined",Execution:&work,Resources:[]tk.Resource{after}}} {record(e)}
 if err=tk.CheckDrain(tk.DrainEvidence{Label:"external actual file",Execution:work,Trace:snapshot()});err!=nil {t.Fatal(err)}
 j,_=tk.NewJournal(64)
 for _,e:=range []tk.Event{{Stage:"candidate.begin"},{Stage:"candidate.prepared",Resources:[]tk.Resource{before}},{Stage:"original.served"},{Stage:"candidate.end",Resources:[]tk.Resource{after}},{Stage:"commit"},{Stage:"enable"},{Stage:"business.enter"}} {record(e)}
 topology:=tk.Topology{Forest:"owned",Generation:0,Fingerprint:"known fixture routing"}
 if err=tk.CheckAdmission(tk.AdmissionEvidence{Label:"external",Before:topology,After:topology,Trace:snapshot()});err!=nil {t.Fatal(err)}
 a:=tk.Execution{Scope:scope,Task:"task",Step:"step",Attempt:"1",Process:"a",Instance:"a"};b:=a;b.Process="b";b.Instance="b"
 effect:=tk.Effect{Namespace:"artifact",Operation:"operation",InputDigest:"input",Receipt:"receipt",ContentDigest:"result",OriginProcess:"a",OriginVersion:"v1"}
 identity:=tk.IdentityEvidence{Label:"external",Original:a,Delivery:b,OriginalEffect:effect,DeliveredEffect:effect,Changed:[]tk.IdentityField{tk.Process,tk.Instance}}
 if err=tk.CheckIdentity(identity);err!=nil {t.Fatal(err)}
 identity.DeliveredEffect.OriginProcess="b"
 if err=tk.CheckIdentity(identity);!errors.Is(err,tk.ErrViolation){t.Fatal("invalid origin accepted",err)}
}
'''


def main():
    env = dict(os.environ, GOWORK="off")
    if env.get("GOSUMDB", "sum.golang.org") == "off":
        raise RuntimeError("published acceptance requires checksum verification")
    with tempfile.TemporaryDirectory(prefix="forest-testkit-published-") as temporary:
        directory = Path(temporary)

        def run(*args, capture=False):
            return subprocess.run(args, cwd=directory, env=env, check=True,
                                  text=True, encoding="utf-8",
                                  stdout=subprocess.PIPE if capture else None).stdout

        run("go", "mod", "init", "external.example/testkit-consumer")
        (directory / "consumer_test.go").write_text(CONSUMER, encoding="utf-8")
        run("go", "get", MODULE + "@" + VERSION)
        run("go", "mod", "tidy")
        run("go", "mod", "verify")
        payload = run("go", "list", "-m", "-json", "all", capture=True)
        decoder, modules = json.JSONDecoder(), []
        while payload.strip():
            payload = payload.lstrip()
            module, position = decoder.raw_decode(payload)
            payload = payload[position:]
            assert "Replace" not in module, "published consumer has replace"
            modules.append(module)
        dependencies = [m for m in modules if not m.get("Main")]
        assert len(dependencies) == 1, dependencies
        module = dependencies[0]
        assert module["Path"] == MODULE and module["Version"] == VERSION, module
        assert not Path(module["Dir"]).resolve().is_relative_to(ROOT)
        production = run("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", MODULE, capture=True)
        assert production.strip().splitlines() == [MODULE], production
        run("go", "vet", "./...")
        run("go", "test", *(["-race"] if os.name != "nt" else []), "./...", "-count=3")
        print("PASS: fixed TestKit + checksums + no replace/Core/framework dependency + four public mechanisms", flush=True)


if __name__ == "__main__":
    main()
