package postgres

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	inference "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/publication"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/quota"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/resources"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/work"
)

// PostgreSQL and the application/worker/store are real here. Only external
// authorities and the Kubernetes/engine runtime are fixtures.
func TestPostgresCreateLifecyclePreservesQuotaResources(t *testing.T) {
	ctx, pool, tenant, service := lifecycleDatabase(t)
	testPostgresCreateLifecycle(t, ctx, pool, tenant, service)
}

func lifecycleDatabase(t *testing.T) (context.Context, *pgxpool.Pool, uuid.UUID, uuid.UUID) {
	t.Helper()
	dsn := os.Getenv("INFERENCE_PG_DSN")
	if dsn == "" {
		t.Skip("set INFERENCE_PG_DSN to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, service := uuid.New(), uuid.New()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, statement := range []string{
			"DELETE FROM inference_managed_gpu_commands WHERE tenant_id=$1",
			"DELETE FROM inference_managed_gpu_resources WHERE tenant_id=$1",
			"UPDATE inference_services SET current_operation_id=NULL WHERE tenant_id=$1",
			"DELETE FROM inference_audit_events WHERE tenant_id=$1",
			"DELETE FROM inference_idempotency_requests WHERE tenant_id=$1",
			"DELETE FROM inference_quota_reservations WHERE tenant_id=$1",
			"DELETE FROM inference_publications WHERE tenant_id=$1",
			"DELETE FROM inference_observation_events WHERE tenant_id=$1",
			"DELETE FROM inference_runtime_bindings WHERE tenant_id=$1",
			"DELETE FROM inference_resource_work WHERE tenant_id=$1",
			"DELETE FROM inference_runtime WHERE tenant_id=$1",
			"DELETE FROM inference_specs WHERE tenant_id=$1",
			"DELETE FROM inference_operations WHERE tenant_id=$1",
			"DELETE FROM inference_services WHERE tenant_id=$1",
		} {
			if _, err := pool.Exec(cleanupCtx, statement, tenant); err != nil {
				t.Errorf("cleanup test tenant: %v", err)
			}
		}
	})
	return ctx, pool, tenant, service
}

func testPostgresCreateLifecycle(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenant, service uuid.UUID) {
	t.Helper()
	resourceJSON := []byte(`{"requests":{"cpu":"2","memory":"8Gi","nvidia.com/gpu":"1"},"limits":{"cpu":"4","memory":"16Gi","nvidia.com/gpu":"1"}}`)
	created, err := NewRepository(pool).CreateService(ctx, CreateAggregateInput{
		TenantID: tenant.String(), ServiceID: service.String(), Name: "lifecycle-" + service.String(),
		ModelVersionID: uuid.NewString(), Resources: resourceJSON, RequestHash: "lifecycle-create",
		IdempotencyKey: "create", Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := &lifecycleProvider{t: t, pool: pool, resources: resourceJSON}
	for {
		// Reconstruct all local worker objects on each iteration. The next
		// step and reservation must come from PG, not retained Go state.
		store := NewOperationStore(pool)
		runner := &inference.Runner{Store: store, Admission: provider, Model: provider, Quota: provider, Publication: provider, Runtime: provider}
		worker := work.Worker{Store: NewWorkStore(pool, WorkStoreOptions{LeaseSeconds: 2, ObserveSeconds: .001}), Execute: runner.Execute}
		if err := worker.RunOnce(ctx, tenant.String()); err != nil {
			t.Fatalf("execute durable create: %v", err)
		}
		op, err := New(pool).GetOperation(ctx, GetOperationParams{TenantID: pgUUID(tenant), ID: pgUUID(uuid.MustParse(created.OperationID))})
		if err != nil {
			t.Fatal(err)
		}
		if op.Phase == "succeeded" {
			if op.Step != "complete" || !op.CompletedAt.Valid {
				t.Fatalf("incomplete terminal operation: %+v", op)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("create did not finish: %s/%s: %v", op.Phase, op.Step, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	wantCalls := []string{"admit", "reserve", "confirm", "apply_cr", "materialize", "apply_runtime", "observe_runtime", "publish", "confirm_published", "observe_runtime"}
	if !reflect.DeepEqual(provider.calls, wantCalls) {
		t.Fatalf("provider order=%v, want %v", provider.calls, wantCalls)
	}
	reservation, err := New(pool).GetQuotaReservation(ctx, GetQuotaReservationParams{TenantID: pgUUID(tenant), OperationID: pgUUID(uuid.MustParse(created.OperationID))})
	if err != nil || reservation.State != "confirmed" {
		t.Fatalf("reservation=%+v err=%v", reservation, err)
	}
	assertResourcesJSON(t, reservation.RequestedResources, resourceJSON)
	pub, err := New(pool).GetPublication(ctx, GetPublicationParams{TenantID: pgUUID(tenant), ServiceID: pgUUID(service), Generation: 1})
	if err != nil || pub.ObservedPhase != "published" {
		t.Fatalf("publication=%+v err=%v", pub, err)
	}
	read := NewReadUseCase(pool)
	resource, err := read.GetService(ctx, tenant.String(), service.String())
	if err != nil || resource.GetPublicationPhase() != "published" || resource.GetRuntimePhase() != "ready" {
		t.Fatalf("GET after completed create: resource=%v err=%v", resource, err)
	}
	serviceRow, err := New(pool).GetService(ctx, GetServiceParams{TenantID: pgUUID(tenant), ID: pgUUID(service)})
	if err != nil || serviceRow.AppliedGeneration != 1 {
		t.Fatalf("applied generation=%d err=%v, want 1 after observation commit", serviceRow.AppliedGeneration, err)
	}
	listed, _, err := read.ListServices(ctx, tenant.String(), 1, "")
	if err != nil || len(listed) != 1 || listed[0].GetPublicationPhase() != "published" {
		t.Fatalf("LIST after completed create: resources=%v err=%v", listed, err)
	}
	var auditCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM inference_audit_events WHERE tenant_id=$1 AND service_id=$2", tenant, service).Scan(&auditCount); err != nil || auditCount != 10 {
		t.Fatalf("create audit events=%d, want acceptance + claim + 8 transitions; err=%v", auditCount, err)
	}
	// A replacing operation must retain both generations' resource shapes;
	// releasing the previous reservation cannot silently erase its snapshot.
	replacement := resources.Normalized{Requests: map[string]string{"cpu": "3"}, Limits: map[string]string{"cpu": "6"}}
	updated, err := NewUpdateUseCase(NewRepository(pool)).Update(ctx, inference.UpdateInput{
		TenantID: tenant.String(), ServiceID: service.String(), RequestID: "update", RequestHash: "replace",
		ExpectedGeneration: 1, Resources: replacement, Replicas: 1, RuntimeMode: "deployment", WorkerReplicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := NewWorkStore(pool, WorkStoreOptions{LeaseSeconds: 2}).Claim(ctx, tenant.String())
	if err != nil || !ok {
		t.Fatalf("claim replacement: ok=%v err=%v", ok, err)
	}
	loaded, err := NewOperationStore(pool).CurrentOperation(ctx, claimed)
	if err != nil {
		t.Fatal(err)
	}
	previousJSON, err := json.Marshal(loaded.PreviousReservation.RequestedResources)
	if err != nil {
		t.Fatal(err)
	}
	assertResourcesJSON(t, previousJSON, resourceJSON)
	if loaded.Reservation.RequestedResources.Requests["cpu"] != "3" || loaded.PreviousReservation.Generation != "1" {
		t.Fatalf("replacement reservation=%+v previous=%+v", loaded.Reservation, loaded.PreviousReservation)
	}
	if loaded.Reservation.Demand.Units != 1 || loaded.Reservation.Demand.Resources.Requests["cpu"] != "3" {
		t.Fatalf("replacement quota demand=%+v, want one unit of cpu=3", loaded.Reservation.Demand)
	}
	if loaded.PreviousReservation.Demand.Units != 1 || loaded.PreviousReservation.Demand.Resources.Requests["cpu"] != "2" {
		t.Fatalf("previous quota demand=%+v, want one unit of cpu=2", loaded.PreviousReservation.Demand)
	}
	// An incompatible JSON shape must stop execution instead of turning into
	// an empty resource request and bypassing quota enforcement.
	if _, err := pool.Exec(ctx, "UPDATE inference_quota_reservations SET requested_resources=$3::jsonb WHERE tenant_id=$1 AND operation_id=$2", tenant, updated.GetOperation().GetId(), `{"requests":{"cpu":2}}`); err != nil {
		t.Fatal(err)
	}
	if _, err := NewOperationStore(pool).CurrentOperation(ctx, claimed); err == nil || !strings.Contains(err.Error(), "decode quota resource snapshot") {
		t.Fatalf("corrupt quota snapshot was accepted: %v", err)
	}
}

func assertResourcesJSON(t *testing.T, got, want []byte) {
	t.Helper()
	var actual, expected any
	if err := json.Unmarshal(got, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("quota resource payload=%s, want %s", got, want)
	}
}

type lifecycleProvider struct {
	t         *testing.T
	pool      *pgxpool.Pool
	resources []byte
	calls     []string
}

func (p *lifecycleProvider) Admit(context.Context, inference.OperationContext) error {
	p.calls = append(p.calls, "admit")
	return nil
}
func (p *lifecycleProvider) Reserve(_ context.Context, in quota.Reservation) (quota.Reservation, error) {
	p.calls = append(p.calls, "reserve")
	payload, err := json.Marshal(in.RequestedResources)
	if err != nil {
		p.t.Fatal(err)
	}
	assertResourcesJSON(p.t, payload, p.resources)
	in.ReservationID = "external-" + in.OperationID
	return in, nil
}
func (p *lifecycleProvider) Confirm(context.Context, quota.Reservation) error {
	p.calls = append(p.calls, "confirm")
	return nil
}
func (p *lifecycleProvider) Release(context.Context, quota.Reservation) error {
	p.t.Fatal("create released quota")
	return nil
}
func (p *lifecycleProvider) Get(context.Context, string, string) (quota.Reservation, error) {
	return quota.Reservation{}, quota.ErrReservationNotFound
}
func (p *lifecycleProvider) EnsureModel(context.Context, inference.OperationContext) (inference.ModelObservation, error) {
	p.calls = append(p.calls, "materialize")
	return inference.ModelObservation{Known: true, Ready: true}, nil
}
func (p *lifecycleProvider) ApplyCR(ctx context.Context, op inference.OperationContext) error {
	p.calls = append(p.calls, "apply_cr")
	row, err := New(p.pool).GetQuotaReservation(ctx, GetQuotaReservationParams{TenantID: pgUUID(uuid.MustParse(op.TenantID)), OperationID: pgUUID(uuid.MustParse(op.ID))})
	if err != nil || row.State != "confirmed" {
		p.t.Fatalf("CR applied before confirmed reservation: %+v err=%v", row, err)
	}
	assertResourcesJSON(p.t, row.RequestedResources, p.resources)
	return nil
}
func (p *lifecycleProvider) ApplyRuntime(context.Context, inference.OperationContext) error {
	p.calls = append(p.calls, "apply_runtime")
	return nil
}
func (p *lifecycleProvider) ObserveRuntime(ctx context.Context, op inference.OperationContext) (inference.RuntimeObservation, error) {
	p.calls = append(p.calls, "observe_runtime")
	observation := inference.RuntimeObservation{Ready: true, RuntimePhase: "ready", ReadyReplicas: 1, ModelReadyKnown: true, ModelReady: true}
	return observation, nil
}
func (p *lifecycleProvider) DeleteRuntime(context.Context, inference.OperationContext) error {
	p.t.Fatal("create deleted runtime")
	return nil
}
func (p *lifecycleProvider) ObserveAbsence(context.Context, inference.OperationContext) (inference.RuntimeObservation, error) {
	p.t.Fatal("unexpected absence check")
	return inference.RuntimeObservation{}, nil
}
func (p *lifecycleProvider) DeleteCR(context.Context, inference.OperationContext) error {
	p.t.Fatal("create deleted CR")
	return nil
}
func (p *lifecycleProvider) Withdraw(context.Context, publication.Publication) error {
	p.t.Fatal("create withdrew publication")
	return nil
}
func (p *lifecycleProvider) ConfirmWithdrawn(context.Context, publication.Publication) (bool, error) {
	p.t.Fatal("unexpected withdrawal check")
	return false, nil
}
func (p *lifecycleProvider) Publish(ctx context.Context, in publication.Publication) error {
	p.calls = append(p.calls, "publish")
	pub, err := New(p.pool).GetPublication(ctx, GetPublicationParams{TenantID: pgUUID(uuid.MustParse(in.TenantID)), ServiceID: pgUUID(uuid.MustParse(in.ServiceID)), Generation: in.Generation})
	if err != nil || pub.DesiredPhase != "publishing" {
		p.t.Fatalf("publication intent not committed: %+v err=%v", pub, err)
	}
	return nil
}
func (p *lifecycleProvider) ConfirmPublished(context.Context, publication.Publication) (bool, error) {
	p.calls = append(p.calls, "confirm_published")
	return true, nil
}
