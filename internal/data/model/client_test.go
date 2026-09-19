package model

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	modelv1 "github.com/zhangzhe-ctrl/ani-model-service/api/model/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type rpcProbe struct {
	modelv1.ModelServiceClient
	call func(context.Context, *modelv1.GetModelVersionRequest) (*modelv1.GetModelVersionResponse, error)
}

func (p rpcProbe) GetModelVersion(ctx context.Context, req *modelv1.GetModelVersionRequest, _ ...grpc.CallOption) (*modelv1.GetModelVersionResponse, error) {
	return p.call(ctx, req)
}

func TestClientMetadataAndDeadline(t *testing.T) {
	tenant := "11111111-1111-4111-8111-111111111111"
	c := &Client{timeout: time.Second, rpc: rpcProbe{call: func(ctx context.Context, req *modelv1.GetModelVersionRequest) (*modelv1.GetModelVersionResponse, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		if len(md) != 2 || len(md.Get("x-ani-tenant-id")) != 1 || md.Get("x-ani-tenant-id")[0] != tenant || req.TenantId != tenant || req.ModelVersionId != "version" {
			t.Fatalf("unexpected request metadata: %v %v", md, req)
		}
		ids := md.Get("x-ani-request-id")
		if len(ids) != 1 {
			t.Fatal(ids)
		}
		if id, err := uuid.Parse(ids[0]); err != nil || id == uuid.Nil || id.String() != ids[0] {
			t.Fatal(ids)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("unbounded call")
		}
		return &modelv1.GetModelVersionResponse{}, nil
	}}}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-ani-tenant-id", "spoof", "x-ani-actor", "governance:user:1", "authorization", "do-not-forward"))
	if _, err := c.GetModelVersion(ctx, tenant, "version"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", uuid.Nil.String(), "11111111111141118111111111111111", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
		if _, err := c.GetModelVersion(ctx, bad, "version"); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
}
func TestClientCancellationAndError(t *testing.T) {
	c := &Client{timeout: 10 * time.Millisecond, rpc: rpcProbe{call: func(ctx context.Context, _ *modelv1.GetModelVersionRequest) (*modelv1.GetModelVersionResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}}
	tenant := "11111111-1111-4111-8111-111111111111"
	if out, err := c.GetModelVersion(context.Background(), tenant, "v"); out != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v %v", out, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetModelVersion(ctx, tenant, "v"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := New("", "", "", "", time.Second); err == nil {
		t.Fatal("accepted missing config")
	}
}
