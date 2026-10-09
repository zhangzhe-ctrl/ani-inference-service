package governance

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	quotav1 "github.com/zhangzhe-ctrl/ani-governance/api/quota/gen/go/quota/service/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type originalStore struct {
	original gpu.RefundContext
	err      error
}

func (s originalStore) LoadRefundContext(_ context.Context, tenant, resource, create string) (gpu.RefundContext, error) {
	if s.err != nil {
		return gpu.RefundContext{}, s.err
	}
	return s.original, nil
}

func refundInput(t *testing.T) (gpu.ReleaseNotification, gpu.RefundContext) {
	t.Helper()
	const tenant = "10000000-0000-4000-8000-000000000001"
	const resource = "10000000-0000-4000-8000-000000000002"
	const create = "10000000-0000-4000-8000-000000000003"
	request := &gpu.Request{ClusterID: tenant, PoolID: resource, ProfileID: create, ProfileVersion: 1, Replicas: 1, DevicesPerReplica: 1, ContainerName: "kserve-container"}
	plan := &gpu.Plan{SchemaVersion: 1, Request: request, Profile: &gpu.Profile{ProfileID: create, ProfileVersion: 1, Spec: &gpu.ProfileSpec{Mode: 2, SharedMemoryMiB: 1024}}, Encoding: &gpu.MemoryEncoding{MemoryBlockMiB: 256, MemoryBlocksPerDevice: 4, SharedMemoryMiB: 1024}, Totals: &gpu.ResourceTotals{LogicalDeviceCount: 1, SharedMemoryMiB: 1024}, Runtime: &gpu.RuntimeFragment{SchedulerName: "volcano"}}
	digest, err := gpu.PlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.ResolutionDigest = digest
	charge := gpu.OriginalCharge{ChargeID: "10000000-0000-4000-8000-000000000004", QuotaCode: "gpu.shared_memory_mib", OriginalUnits: 1024}
	original := gpu.RefundContext{TenantID: tenant, ResourceID: resource, OriginalCreateOperationID: create, DeleteOperationID: "10000000-0000-4000-8000-000000000005", OwnerService: "ani-inference", MeteringVersion: "gpu-metering-v1", Plan: plan, GPUCharges: []gpu.OriginalCharge{charge}, Charges: []gpu.OriginalCharge{charge}}
	n := gpu.ReleaseNotification{TenantID: tenant, ResourceID: resource, OriginalCreateOperationID: create, ReleaseEventID: "10000000-0000-4000-8000-000000000006", Reason: gpu.ResourceReleased, Items: []gpu.ReleaseItem{{ChargeID: charge.ChargeID, QuotaCode: charge.QuotaCode, ReleasedTotal: 1024}}, ResourceRefs: []string{resource}}
	return n, original
}

type releaseStub struct {
	requests []*quotav1.ReportQuotaReleaseRequest
	reply    *quotav1.ReportQuotaReleaseResponse
	err      error
	wait     bool
}

func (s *releaseStub) ReportQuotaRelease(ctx context.Context, req *quotav1.ReportQuotaReleaseRequest, _ ...grpc.CallOption) (*quotav1.ReportQuotaReleaseResponse, error) {
	s.requests = append(s.requests, proto.Clone(req).(*quotav1.ReportQuotaReleaseRequest))
	if s.wait {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return s.reply, s.err
}

func TestClientValidatesContextAndStableRetryWithoutChangingEvent(t *testing.T) {
	n, original := refundInput(t)
	rpc := &releaseStub{reply: &quotav1.ReportQuotaReleaseResponse{Items: []*quotav1.QuotaReleaseResult{{ChargeId: n.Items[0].ChargeID, AppliedDelta: 1024, ReleasedTotal: 1024}}}}
	client, err := NewClient(rpc, originalStore{original: original}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReportQuotaRelease(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	rpc.reply.Items[0].AppliedDelta = 0
	if _, err := client.ReportQuotaRelease(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if len(rpc.requests) != 2 || !proto.Equal(rpc.requests[0], rpc.requests[1]) || rpc.requests[0].OperationId != original.OriginalCreateOperationID || rpc.requests[0].Items[0].ReleasedTotal != 1024 {
		t.Fatalf("retry changed original notification: %v", rpc.requests)
	}
	n.Items[0].ReleasedTotal = original.Plan.Encoding.MemoryBlocksPerDevice
	if _, err := client.ReportQuotaRelease(context.Background(), n); !errors.Is(err, gpu.ErrInvalidRefund) || len(rpc.requests) != 2 {
		t.Fatalf("invalid units reached Governance: %v", err)
	}
}

func TestClientPropagatesLookupRPCAndIncompleteResponseErrors(t *testing.T) {
	n, original := refundInput(t)
	lookupErr := errors.New("persistent snapshot unavailable")
	client, _ := NewClient(&releaseStub{}, originalStore{err: lookupErr}, time.Second)
	if _, err := client.ReportQuotaRelease(context.Background(), n); !errors.Is(err, lookupErr) {
		t.Fatalf("lookup error lost: %v", err)
	}
	rpc := &releaseStub{err: status.Error(codes.Unavailable, "receiver unavailable")}
	client, _ = NewClient(rpc, originalStore{original: original}, time.Second)
	if _, err := client.ReportQuotaRelease(context.Background(), n); status.Code(err) != codes.Unavailable {
		t.Fatalf("RPC error lost: %v", err)
	}
	rpc.err = nil
	rpc.reply = &quotav1.ReportQuotaReleaseResponse{}
	if _, err := client.ReportQuotaRelease(context.Background(), n); !errors.Is(err, gpu.ErrInvalidRefund) {
		t.Fatalf("missing result accepted: %v", err)
	}
	rpc.wait = true
	client.timeout = 10 * time.Millisecond
	if _, err := client.ReportQuotaRelease(context.Background(), n); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("fixed timeout not enforced: %v", err)
	}
}

type mtlsReleaseServer struct {
	quotav1.UnimplementedQuotaReleaseServiceServer
	mu       sync.Mutex
	requests []*quotav1.ReportQuotaReleaseRequest
}

func (s *mtlsReleaseServer) ReportQuotaRelease(ctx context.Context, req *quotav1.ReportQuotaReleaseRequest) (*quotav1.ReportQuotaReleaseResponse, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing TLS")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || info.State.Version != tls.VersionTLS13 || len(info.State.VerifiedChains) == 0 {
		return nil, status.Error(codes.Unauthenticated, "verified TLS1.3 required")
	}
	if names := info.State.PeerCertificates[0].DNSNames; len(names) != 1 || names[0] != "inference.refund.test.invalid" {
		return nil, status.Error(codes.PermissionDenied, "wrong owner DNS SAN")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, proto.Clone(req).(*quotav1.ReportQuotaReleaseRequest))
	out := &quotav1.ReportQuotaReleaseResponse{}
	for _, it := range req.Items {
		delta := it.ReleasedTotal
		if len(s.requests) > 1 {
			delta = 0
		}
		out.Items = append(out.Items, &quotav1.QuotaReleaseResult{ChargeId: it.ChargeId, AppliedDelta: delta, ReleasedTotal: it.ReleasedTotal})
	}
	return out, nil
}

// These isolated certificates exercise the real TLS-only adapter transport.
// The root suite separately calls the actual Governance receiver and PG ledger.
func testCertificate(t *testing.T, directory, name string, serial int64, ca *x509.Certificate, caKey ed25519.PrivateKey, server bool) (tls.Certificate, *x509.Certificate, ed25519.PrivateKey, string, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, DNSNames: []string{name}}
	if ca == nil {
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.KeyUsage |= x509.KeyUsageCertSign
		ca = template
		caKey = key
	} else if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, pub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := filepath.Join(directory, name+".pem"), filepath.Join(directory, name+".key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encodedKey}), 0600); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	return pair, parsed, key, certFile, keyFile
}

func TestDialUsesRealMutualTLSAndRejectsWrongCANameAndOwner(t *testing.T) {
	directory := t.TempDir()
	_, ca, caKey, caFile, _ := testCertificate(t, directory, "ca", 1, nil, nil, false)
	serverCert, _, _, _, _ := testCertificate(t, directory, "quota.refund.test.invalid", 2, ca, caKey, true)
	_, _, _, clientCert, clientKey := testCertificate(t, directory, "inference.refund.test.invalid", 3, ca, caKey, false)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := &mtlsReleaseServer{}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})))
	quotav1.RegisterQuotaReleaseServiceServer(srv, rpc)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() { srv.Stop(); _ = listener.Close() })
	n, original := refundInput(t)
	cfg := Config{Endpoint: listener.Addr().String(), Timeout: time.Second, TLS: TLSConfig{CAFile: caFile, CertFile: clientCert, KeyFile: clientKey, ServerName: "quota.refund.test.invalid"}}
	client, closeClient, err := Dial(context.Background(), cfg, originalStore{original: original})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = closeClient() })
	first, err := client.ReportQuotaRelease(context.Background(), n)
	if err != nil || first.Items[0].AppliedDelta != 1024 {
		t.Fatalf("mTLS first receipt=%v err=%v", first, err)
	}
	replay, err := client.ReportQuotaRelease(context.Background(), n)
	if err != nil || replay.Items[0].AppliedDelta != 0 {
		t.Fatalf("mTLS replay=%v err=%v", replay, err)
	}
	bad := cfg
	bad.Timeout = 50 * time.Millisecond
	bad.TLS.ServerName = "wrong.refund.test.invalid"
	if c, closeBad, err := Dial(context.Background(), bad, originalStore{original: original}); err == nil {
		_ = closeBad()
		_ = c
		t.Fatal("wrong server name accepted")
	}
	_, _, _, otherCA, _ := testCertificate(t, directory, "other-ca", 4, nil, nil, false)
	bad = cfg
	bad.Timeout = 50 * time.Millisecond
	bad.TLS.CAFile = otherCA
	if _, closeBad, err := Dial(context.Background(), bad, originalStore{original: original}); err == nil {
		_ = closeBad()
		t.Fatal("wrong CA accepted")
	}
	_, _, _, wrongCert, wrongKey := testCertificate(t, directory, "wrong-owner.refund.test.invalid", 5, ca, caKey, false)
	bad = cfg
	bad.TLS.CertFile = wrongCert
	bad.TLS.KeyFile = wrongKey
	wrong, closeWrong, err := Dial(context.Background(), bad, originalStore{original: original})
	if err != nil {
		t.Fatal(err)
	}
	defer closeWrong()
	if _, err := wrong.ReportQuotaRelease(context.Background(), n); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong owner accepted: %v", err)
	}
}
