package inference

import (
	"context"
	"errors"

	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
)

var (
	ErrNotFound            = errors.New("inference service not found")
	ErrGenerationConflict  = errors.New("inference generation changed")
	ErrOperationActive     = errors.New("inference service has an active operation")
	ErrInvalidState        = errors.New("inference command is not valid for the current state")
	ErrIdempotencyConflict = errors.New("idempotency key already used with a different request")
)

type CommandInput struct {
	TenantID, RequestID, Actor, ServiceID, Kind, RequestHash string
	ExpectedGeneration                                       int64
	ManagedGPU                                               *ManagedGPUCommand
}

type CommandUseCase interface {
	Command(context.Context, CommandInput) (*inferencev1.OperationResponse, error)
}
