package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/governance"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/postgres"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/server"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestManagedOwnerProcess is an explicitly opted-in cross-repository test
// subprocess. Acceptance/service/PG/auth are real; external workload execution
// is paused, so no ACK can be mistaken for actual workload readiness/cleanup.
func TestManagedOwnerProcess(t *testing.T) {
	if os.Getenv("INFERENCE_MANAGED_PROCESS") != "1" {
		t.Skip("cross-repository owner subprocess is opt-in")
	}
	pool := managedProcessPool(t)
	configBytes, err := os.ReadFile(requiredProcessEnv(t, "INFERENCE_MANAGED_OWNER_CONFIG_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	var bc inferencev1.Bootstrap
	if err := protojson.Unmarshal(configBytes, &bc); err != nil {
		t.Fatal(err)
	}
	if bc.GetServer().GetGrpc() == nil {
		t.Fatal("server.grpc config is required")
	}
	if bc.Server.Grpc.GetTimeout() == nil {
		bc.Server.Grpc.Timeout = durationpb.New(5 * time.Second)
	}
	if bc.Server.Grpc.GetNetwork() == "" {
		bc.Server.Grpc.Network = "tcp"
	}
	if _, enabled, err := governanceRefundConfig(bc.GetManagedGpu()); err != nil || !enabled {
		t.Fatalf("managed identity configuration required: %v", err)
	}
	repo := postgres.NewRepository(pool)
	owner := service.NewInferenceServerWithAll(postgres.NewCreateUseCase(repo), postgres.NewReadUseCase(pool), postgres.NewCommandUseCase(repo), postgres.NewUpdateUseCase(repo))
	grpcServer, err := configuredGovernanceGRPCServer(bc.Server.Grpc, bc.GetManagedGpu(), owner, server.DirectTenantMiddleware())
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := grpcServer.Endpoint()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- grpcServer.Start(ctx) }()
	if err := os.WriteFile(requiredProcessEnv(t, "INFERENCE_MANAGED_READY"), []byte(endpoint.Host), 0600); err != nil {
		_ = grpcServer.Stop(context.Background())
		t.Fatal(err)
	}
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := grpcServer.Stop(stopCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-startErr:
			if err != nil {
				t.Fatal(err)
			}
		case <-stopCtx.Done():
			t.Fatal("owner subprocess failed to stop")
		}
	}
}

type injectedRefundResult struct {
	Receipt *gpu.ReleaseReceipt `json:"receipt,omitempty"`
	Code    string              `json:"code"`
	Error   string              `json:"error,omitempty"`
}

// TestInjectedQuotaRefund takes an explicit durable completion-notice fixture.
// It uses the accepted PG context and actual TLS adapter against the real
// Governance process. The test does not create a lifecycle fact or trigger.
func TestInjectedQuotaRefund(t *testing.T) {
	if os.Getenv("INFERENCE_REFUND_NOTIFICATION_FILE") == "" {
		t.Skip("cross-repository injected refund subprocess is opt-in")
	}
	pool := managedProcessPool(t)
	var cfg governance.Config
	readProcessJSON(t, requiredProcessEnv(t, "INFERENCE_REFUND_CONFIG_FILE"), &cfg)
	var notice gpu.ReleaseNotification
	readProcessJSON(t, requiredProcessEnv(t, "INFERENCE_REFUND_NOTIFICATION_FILE"), &notice)
	client, closeClient, err := governance.Dial(context.Background(), cfg, postgres.NewRepository(pool))
	if err != nil {
		writeInjectedRefundResult(t, gpu.ReleaseReceipt{}, err)
		t.Fatal(err)
	}
	defer closeClient()
	owner := service.NewInferenceServer()
	owner.SetManagedGPURefundReporter(client)
	receipt, err := owner.ReportManagedGPUCompletion(context.Background(), notice)
	writeInjectedRefundResult(t, receipt, err)
	if err != nil {
		t.Fatal(err)
	}
}

// TestInspectManagedOwnerProjection follows the accepted PG generation through
// the same renderer used by the production executor. It makes no Kubernetes
// call and does not replace the GPU plan, ref, charges or business parameters.
func TestInspectManagedOwnerProjection(t *testing.T) {
	if os.Getenv("INFERENCE_PROJECTION_RESULT_FILE") == "" {
		t.Skip("cross-repository accepted runtime projection is opt-in")
	}
	pool := managedProcessPool(t)
	tenantID := requiredProcessEnv(t, "INFERENCE_PROJECTION_TENANT")
	resourceID := requiredProcessEnv(t, "INFERENCE_PROJECTION_RESOURCE")
	namespace := os.Getenv("INFERENCE_PROJECTION_NAMESPACE")
	if namespace == "" {
		namespace = "managed-wiring"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	desired, err := postgres.NewRuntimeSource(pool, namespace).CurrentRuntime(ctx, tenantID, resourceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !desired.ManagedGPU || desired.GPUPlan == nil || desired.Resources.GPU == nil {
		t.Fatal("projection requires an actual accepted managed GPU generation")
	}
	var tenantUUID, resourceUUID pgtype.UUID
	if err := tenantUUID.Scan(tenantID); err != nil {
		t.Fatal(err)
	}
	if err := resourceUUID.Scan(resourceID); err != nil {
		t.Fatal(err)
	}
	row, err := postgres.New(pool).GetManagedGPUResource(ctx, postgres.GetManagedGPUResourceParams{TenantID: tenantUUID, ResourceID: resourceUUID})
	if err != nil {
		t.Fatal(err)
	}
	original, err := postgres.NewRepository(pool).LoadRefundContext(ctx, tenantID, resourceID, row.CreateOperationID.String())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original.Plan, desired.GPUPlan) {
		t.Fatal("runtime GPU plan differs from the accepted original command context")
	}
	object, err := kubernetes.RenderKServeRuntime(desired.RuntimeSpec)
	if err != nil {
		t.Fatal(err)
	}
	result := struct {
		Runtime         kubernetes.DesiredRuntime `json:"runtime"`
		OriginalContext gpu.RefundContext         `json:"original_context"`
		Object          map[string]interface{}    `json:"object"`
	}{desired, original, object.Object}
	raw, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(requiredProcessEnv(t, "INFERENCE_PROJECTION_RESULT_FILE"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeInjectedRefundResult(t *testing.T, receipt gpu.ReleaseReceipt, err error) {
	t.Helper()
	result := injectedRefundResult{Code: codes.OK.String()}
	if err != nil {
		result.Code = status.Code(err).String()
		if errors.Is(err, gpu.ErrInvalidRefund) {
			result.Code = codes.InvalidArgument.String()
		}
		result.Error = err.Error()
	} else {
		result.Receipt = &receipt
	}
	encoded, encodeErr := json.Marshal(result)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if writeErr := os.WriteFile(requiredProcessEnv(t, "INFERENCE_REFUND_RESULT_FILE"), encoded, 0600); writeErr != nil {
		t.Fatal(writeErr)
	}
}

func managedProcessPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := requiredProcessEnv(t, "INFERENCE_PG_DSN")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// The root provisions this exclusive database and applies the existing
	// versioned migrations explicitly. No schema is created on process start.
	return pool
}

func requiredProcessEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for opted-in software integration", name)
	}
	return value
}

func readProcessJSON(t *testing.T, path string, destination interface{}) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		t.Fatal(err)
	}
}
