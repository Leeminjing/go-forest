package grpcforest_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	grpcforest "github.com/Leeminjing/go-forest/adapters/grpc"
	"github.com/Leeminjing/go-forest/forest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "google.golang.org/grpc/interop/grpc_testing"
)

func TestNativeTransportCredentials(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "forest-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	x := newFixture(t, false, grpc.Creds(credentials.NewTLS(serverTLS)))
	var target string
	err = x.f.Invoke(context.Background(), func(ctx context.Context) error {
		return forest.Use[grpcforest.Endpoint](ctx, x.f, "grpc", "rpc", func(e grpcforest.Endpoint) error { target = e.Target(); return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	trusted, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(trusted)
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := pb.NewTestServiceClient(connection).UnaryCall(ctx, &pb.SimpleRequest{})
	if err != nil || string(response.GetPayload().GetBody()) != "v1/v1@1" {
		t.Fatal(response, err)
	}
}
