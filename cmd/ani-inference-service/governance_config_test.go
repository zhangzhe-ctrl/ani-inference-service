package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
	"time"

	integrationv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/integration/v1"
	acceleratorv1 "github.com/zhangzhe-ctrl/ani-accelerator-service/api/gen/go/accelerator/v1"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func validManagedConfig() *inferencev1.ManagedGPU {
	return &inferencev1.ManagedGPU{Receiver: &inferencev1.ManagedGPU_Receiver{Address: "managed-listener-address", CaFile: "receiver-ca", CertFile: "receiver-cert", KeyFile: "receiver-key"}, Refund: &inferencev1.ManagedGPU_Refund{Address: "quota-address", CaFile: "quota-ca", CertFile: "quota-cert", KeyFile: "quota-key", ServerName: "quota-service", Timeout: durationpb.New(3 * time.Second)}}
}

func TestManagedGPUConfigIsTypedCompleteAndOptional(t *testing.T) {
	if _, enabled, err := governanceRefundConfig(nil); err != nil || enabled {
		t.Fatalf("CPU configuration: enabled=%v err=%v", enabled, err)
	}
	if _, enabled, err := governanceRefundConfig(&inferencev1.ManagedGPU{Refund: &inferencev1.ManagedGPU_Refund{Timeout: durationpb.New(5 * time.Second)}}); err != nil || enabled {
		t.Fatalf("empty YAML config: enabled=%v err=%v", enabled, err)
	}
	cfg, enabled, err := governanceRefundConfig(validManagedConfig())
	if err != nil || !enabled || cfg.Timeout != 3*time.Second || cfg.TLS.ServerName != "quota-service" {
		t.Fatalf("managed config=%v enabled=%v err=%v", cfg, enabled, err)
	}
	for _, test := range []struct {
		name  string
		alter func(*inferencev1.ManagedGPU)
	}{
		{"missing-address", func(c *inferencev1.ManagedGPU) { c.Refund.Address = "" }},
		{"missing-client-ca", func(c *inferencev1.ManagedGPU) { c.Refund.CaFile = "" }},
		{"missing-client-cert", func(c *inferencev1.ManagedGPU) { c.Refund.CertFile = "" }},
		{"missing-client-key", func(c *inferencev1.ManagedGPU) { c.Refund.KeyFile = "" }},
		{"missing-server-name", func(c *inferencev1.ManagedGPU) { c.Refund.ServerName = "" }},
		{"missing-receiver", func(c *inferencev1.ManagedGPU) { c.Receiver = nil }},
		{"missing-receiver-address", func(c *inferencev1.ManagedGPU) { c.Receiver.Address = "" }},
		{"negative-timeout", func(c *inferencev1.ManagedGPU) { c.Refund.Timeout = durationpb.New(-time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := validManagedConfig()
			test.alter(c)
			if _, _, err := governanceRefundConfig(c); err == nil {
				t.Fatal("incomplete managed config accepted")
			}
		})
	}
}

func trustedPeerContext(t *testing.T, uriNames []string, verified bool) context.Context {
	t.Helper()
	leaf := &x509.Certificate{}
	for _, name := range uriNames {
		uri, err := url.Parse(name)
		if err != nil {
			t.Fatal(err)
		}
		leaf.URIs = append(leaf.URIs, uri)
	}
	state := tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{leaf}}
	if verified {
		state.VerifiedChains = [][]*x509.Certificate{{leaf}}
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

func TestGovernanceReceiverBindsAuthenticatedAttachmentTenantAndIdentity(t *testing.T) {
	const tenant = "10000000-0000-4000-8000-000000000001"
	request := &inferencev1.CreateInferenceServiceRequest{GpuOwnerAttachment: &integrationv1.GpuOwnerCreateAttachment{Ref: &acceleratorv1.GpuUsageRef{TenantId: tenant}}}
	called := false
	handler := governanceIdentityMiddleware()(func(ctx context.Context, _ interface{}) (interface{}, error) {
		called = true
		if !gpu.GovernanceIdentity(ctx) {
			t.Fatal("identity marker missing")
		}
		if got, ok := service.TenantID(ctx); !ok || got != tenant {
			t.Fatalf("tenant=%s present=%v", got, ok)
		}
		return nil, nil
	})
	ctx := trustedPeerContext(t, []string{governanceCallerURI}, true)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-ani-tenant-id", tenant))
	if _, err := handler(ctx, request); err != nil || !called {
		t.Fatalf("trusted command rejected: %v", err)
	}
	called = false
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-ani-tenant-id", "10000000-0000-4000-8000-000000000099"))
	if _, err := handler(ctx, request); status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("metadata changed tenant: %v", err)
	}
}

func TestGovernanceReceiverRejectsMetadataOnlyWrongOrAmbiguousCertificate(t *testing.T) {
	handler := governanceIdentityMiddleware()(func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("unauthorized command reached handler")
		return nil, nil
	})
	for _, test := range []struct {
		name string
		ctx  context.Context
		want codes.Code
	}{
		{"metadata-only", metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-ani-owner-service", "ani-governance")), codes.Unauthenticated},
		{"unverified-ca", trustedPeerContext(t, []string{governanceCallerURI}, false), codes.Unauthenticated},
		{"wrong-uri", trustedPeerContext(t, []string{"spiffe://ani.internal/service/ani-inference"}, true), codes.PermissionDenied},
		{"multiple-uri", trustedPeerContext(t, []string{governanceCallerURI, "spiffe://ani.internal/service/other"}, true), codes.PermissionDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := handler(test.ctx, &inferencev1.CreateInferenceServiceRequest{}); status.Code(err) != test.want {
				t.Fatalf("code=%v err=%v want=%v", status.Code(err), err, test.want)
			}
		})
	}
}
