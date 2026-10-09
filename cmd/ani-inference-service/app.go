package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	kratos "github.com/go-kratos/kratos/v3"
	kratosTransport "github.com/go-kratos/kratos/v3/transport"
	kratosgrpc "github.com/go-kratos/kratos/v3/transport/grpc"
	kratoshttp "github.com/go-kratos/kratos/v3/transport/http"

	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/work"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/server"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
)

func buildAppWithAllDependenciesAndBackground(bc *inferencev1.Bootstrap, logger *slog.Logger, create service.CreateUseCase, read inferencebiz.ReadUseCase, command inferencebiz.CommandUseCase, update inferencebiz.UpdateUseCase, background ...kratosTransport.Server) (*kratos.App, error) {
	return buildAppWithAllDependenciesAndManagedGPU(bc, logger, create, read, command, update, nil, background...)
}

func buildAppWithAllDependenciesAndManagedGPU(bc *inferencev1.Bootstrap, logger *slog.Logger, create service.CreateUseCase, read inferencebiz.ReadUseCase, command inferencebiz.CommandUseCase, update inferencebiz.UpdateUseCase, refund gpu.RefundReporter, background ...kratosTransport.Server) (*kratos.App, error) {
	if err := bc.Validate(); err != nil {
		return nil, err
	}
	if _, enabled, err := governanceRefundConfig(bc.GetManagedGpu()); err != nil {
		return nil, err
	} else if enabled && refund == nil {
		return nil, errors.New("managed GPU receiver requires the real refund reporter")
	}
	readiness := server.NewReadiness()
	observability, err := server.NewObservability(Name, Version, readiness)
	if err != nil {
		return nil, err
	}
	middlewares := observability.ServerMiddleware(logger)
	for _, backgroundServer := range background {
		if sourceProvider, ok := backgroundServer.(interface{ MetricsSource() work.MetricsSource }); ok {
			observability.SetMetricsSource(sourceProvider.MetricsSource())
		}
	}
	owner := service.NewInferenceServerWithAll(create, read, command, update)
	owner.SetManagedGPURefundReporter(refund)
	grpcServer := server.NewGRPCServer(bc.Server.Grpc, middlewares...)
	inferencev1.RegisterInferenceServiceManagerServer(grpcServer, owner)
	managedServer, err := configuredGovernanceGRPCServer(bc.Server.Grpc, bc.GetManagedGpu(), owner, middlewares...)
	if err != nil {
		_ = observability.Shutdown(context.Background())
		return nil, err
	}
	readyOnStart := readyOnStartForBackground(background)
	if managedServer != nil {
		background = append(background, managedServer)
	}
	referenceReader, _ := read.(inferencebiz.ModelReferenceReader)
	inferencev1.RegisterModelReferenceServiceServer(grpcServer, service.NewModelReferenceServer(referenceReader))
	adminServer := server.NewAdminServer(bc.Server.Admin, readiness, observability.Gatherer(), middlewares...)
	return newApp(logger, grpcServer, adminServer, readiness, observability, bc.Server.ShutdownTimeout.AsDuration(), readyOnStart, background...), nil
}

func readyOnStartForBackground(background []kratosTransport.Server) bool {
	return len(background) == 0
}

func newApp(
	logger *slog.Logger,
	grpcServer *kratosgrpc.Server,
	adminServer *kratoshttp.Server,
	readiness *server.Readiness,
	observability *server.Observability,
	stopTimeout time.Duration,
	readyOnStart bool,
	background ...kratosTransport.Server,
) *kratos.App {
	servers := make([]kratosTransport.Server, 0, 2+len(background))
	servers = append(servers, grpcServer, adminServer)
	servers = append(servers, background...)
	for _, backgroundServer := range background {
		if readyAware, ok := backgroundServer.(interface{ SetReadyCallback(func()) }); ok {
			readyAware.SetReadyCallback(func() { readiness.Set(true) })
		}
	}
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Logger(logger),
		kratos.Server(servers...),
		kratos.AfterStart(func(context.Context) error {
			if readyOnStart {
				readiness.Set(true)
			}
			return nil
		}),
		kratos.BeforeStop(func(context.Context) error {
			readiness.Set(false)
			return nil
		}),
		kratos.AfterStop(observability.Shutdown),
		kratos.StopTimeout(stopTimeout),
	)
}
