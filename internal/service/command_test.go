package service

import (
	"context"
	"errors"
	"testing"

	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeCommand struct {
	inputs []inferencebiz.CommandInput
	out    *inferencev1.OperationResponse
	err    error
}

func (f *fakeCommand) Command(_ context.Context, in inferencebiz.CommandInput) (*inferencev1.OperationResponse, error) {
	f.inputs = append(f.inputs, in)
	return f.out, f.err
}

type commandRPC func(context.Context, *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error)

func commandRPCs(server *InferenceServer) map[string]commandRPC {
	return map[string]commandRPC{
		"start":   server.StartInferenceService,
		"stop":    server.StopInferenceService,
		"restart": server.RestartInferenceService,
		"delete": func(ctx context.Context, req *inferencev1.ServiceCommandRequest) (*inferencev1.OperationResponse, error) {
			if req == nil {
				return server.DeleteInferenceService(ctx, nil)
			}
			return server.DeleteInferenceService(ctx, &inferencev1.DeleteInferenceServiceRequest{RequestId: req.RequestId, ResourceId: req.ResourceId, ExpectedGeneration: req.ExpectedGeneration})
		},
	}
}

func TestCommandsRequireIdentityAndGeneration(t *testing.T) {
	for kind, rpc := range commandRPCs(NewInferenceServer()) {
		for _, tc := range []struct {
			name string
			ctx  context.Context
			req  *inferencev1.ServiceCommandRequest
			code codes.Code
		}{
			{"nil request", WithTenantID(context.Background(), "tenant-a"), nil, codes.InvalidArgument},
			{"no identity", context.Background(), &inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "s", ExpectedGeneration: 1}, codes.Unauthenticated},
			{"no request id", WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{ResourceId: "s", ExpectedGeneration: 1}, codes.InvalidArgument},
			{"no resource id", WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{RequestId: "r", ExpectedGeneration: 1}, codes.InvalidArgument},
			{"zero generation", WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "s"}, codes.InvalidArgument},
			{"negative generation", WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "s", ExpectedGeneration: -1}, codes.InvalidArgument},
			{"unconfigured", WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "s", ExpectedGeneration: 1}, codes.FailedPrecondition},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				if _, err := rpc(tc.ctx, tc.req); status.Code(err) != tc.code {
					t.Fatalf("code=%v want=%v err=%v", status.Code(err), tc.code, err)
				}
			})
		}
	}
}

func TestCommandsDelegateIdentityAndStableActionHash(t *testing.T) {
	uc := &fakeCommand{out: &inferencev1.OperationResponse{Operation: &inferencev1.Operation{Id: "op-1", Phase: "pending"}}}
	server := NewInferenceServerWithCommands(nil, nil, uc)
	ctx := WithActor(WithTenantID(context.Background(), "tenant-a"), "user-42")
	req := &inferencev1.ServiceCommandRequest{RequestId: "r-1", ResourceId: "svc-1", ExpectedGeneration: 3}
	hashes := map[string]bool{}
	for kind, rpc := range commandRPCs(server) {
		for range 2 {
			out, err := rpc(ctx, req)
			if err != nil || out.GetOperation().GetId() != "op-1" {
				t.Fatalf("%s: out=%v err=%v", kind, out, err)
			}
		}
		in := uc.inputs[len(uc.inputs)-1]
		if in.TenantID != "tenant-a" || in.Actor != "user-42" || in.RequestID != "r-1" || in.ServiceID != "svc-1" || in.ExpectedGeneration != 3 || in.Kind != kind {
			t.Fatalf("%s: input=%+v", kind, in)
		}
		if len(in.RequestHash) != 64 || in.RequestHash != uc.inputs[len(uc.inputs)-2].RequestHash || hashes[in.RequestHash] {
			t.Fatalf("%s: unstable or action-colliding hash=%q", kind, in.RequestHash)
		}
		hashes[in.RequestHash] = true
	}
	req.ExpectedGeneration++
	if _, err := server.StartInferenceService(ctx, req); err != nil {
		t.Fatal(err)
	}
	if hashes[uc.inputs[len(uc.inputs)-1].RequestHash] {
		t.Fatal("generation change must change request hash")
	}
}

func TestCommandsRequireAsyncOperationAndPropagateFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  *inferencev1.OperationResponse
		err  error
		code codes.Code
	}{
		{"nil response", nil, nil, codes.Internal},
		{"nil operation", &inferencev1.OperationResponse{}, nil, codes.Internal},
		{"usecase error", nil, status.Error(codes.Aborted, "generation changed"), codes.Aborted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc := &fakeCommand{out: tc.out, err: tc.err}
			server := NewInferenceServerWithCommands(nil, nil, uc)
			_, err := server.StartInferenceService(WithTenantID(context.Background(), "tenant-a"), &inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "svc", ExpectedGeneration: 1})
			if status.Code(err) != tc.code || tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("code=%v want=%v err=%v", status.Code(err), tc.code, err)
			}
		})
	}
}

func TestCommandsMapDomainErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{inferencebiz.ErrNotFound, codes.NotFound},
		{inferencebiz.ErrGenerationConflict, codes.Aborted},
		{inferencebiz.ErrOperationActive, codes.FailedPrecondition},
		{inferencebiz.ErrInvalidState, codes.FailedPrecondition},
		{inferencebiz.ErrIdempotencyConflict, codes.AlreadyExists},
	} {
		uc := &fakeCommand{err: tc.err}
		_, err := NewInferenceServerWithCommands(nil, nil, uc).StartInferenceService(
			WithTenantID(context.Background(), "tenant-a"),
			&inferencev1.ServiceCommandRequest{RequestId: "r", ResourceId: "svc", ExpectedGeneration: 1})
		if status.Code(err) != tc.code {
			t.Fatalf("err=%v code=%v want=%v", err, status.Code(err), tc.code)
		}
	}
}
