package ecosystem

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	nt "github.com/Leeminjing/go-forest/adapters/nats"
	"github.com/Leeminjing/go-forest/forest"
	tk "github.com/Leeminjing/go-forest/testkit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConformanceNewTransportDoesNotChangeBusinessEffect(t *testing.T) {
	f := newConf(t)
	f.submit(t, "http", "duplicate")
	firstSeq := f.relay(t, "duplicate")
	f.enable(t)
	first := f.await(t, firstSeq)
	f.acked(t, firstSeq)
	publish := func(data []byte) uint64 {
		var ack *jetstream.PubAck
		err := f.r.Forest.Invoke(f.ctx, func(c context.Context) error {
			return forest.Use[nt.Publisher](c, f.r.Forest, "app/publisher", "publisher", func(p nt.Publisher) error {
				var err error
				ack, err = p.Publish(c, &nats.Msg{Subject: f.o.Namespace, Data: data, Header: nats.Header{jetstream.MsgIDHeader: []string{confNonce()}}})
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		return ack.Sequence
	}
	secondSeq := publish(first.Message.Data)
	second := f.await(t, secondSeq)
	f.acked(t, secondSeq)
	if secondSeq == firstSeq || second.Execution.Task == first.Execution.Task || second.Message.ID == first.Message.ID {
		t.Fatal("new transport conflated with original redelivery", first, second)
	}
	receipt, n, rows := confReadDatabase(t, f.ctx, f.o, "duplicate")
	if first.Receipt != second.Receipt || receipt != first.Receipt || n != 1 || rows != 1 {
		t.Fatal("independent duplicate repeated effect", receipt, n, rows)
	}
	independent := tk.IdentityEvidence{Original: first.Execution, Delivery: second.Execution, OriginalEffect: first.Receipt.effect(), DeliveredEffect: second.Receipt.effect(), Changed: []tk.IdentityField{tk.Call}}
	if err := tk.CheckIdentity(independent); !errors.Is(err, tk.ErrViolation) {
		t.Fatal("distinct messages accepted as same logical delivery", err)
	}
	var conflicting event
	if err := json.Unmarshal(first.Message.Data, &conflicting); err != nil {
		t.Fatal(err)
	}
	conflicting.Input = "different input"
	raw, err := json.Marshal(conflicting)
	if err != nil {
		t.Fatal(err)
	}
	conflictSeq := publish(raw)
	confUntil(t, f.ctx, func() bool { return errors.Is(f.evidence.rejection(conflictSeq), ErrConflict) })
	if err = f.quiesce(f.ctx); err != nil {
		t.Fatal(err)
	}
	receiptAfter, n, rows := confReadDatabase(t, f.ctx, f.o, "duplicate")
	if receiptAfter != receipt || n != 1 || rows != 1 {
		t.Fatal("conflict overwrote original", receiptAfter, n, rows)
	}
	broker, err := f.native.Info(f.ctx)
	if err != nil || broker.Delivered.Stream < conflictSeq || broker.AckFloor.Stream >= conflictSeq {
		t.Fatal("conflicting message acknowledged", broker, err)
	}
	if result := f.complete(t, "duplicate"); result.Result != "model v1: 5" {
		t.Fatal(result)
	}
}
