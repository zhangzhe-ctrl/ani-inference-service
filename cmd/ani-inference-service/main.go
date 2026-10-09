package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"
	"github.com/go-kratos/kratos/v3/log"
	kratosTransport "github.com/go-kratos/kratos/v3/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/automaxprocs/maxprocs"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlmanager "sigs.k8s.io/controller-runtime/pkg/manager"

	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	inferencebiz "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/inference"
	bizreconcile "github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/reconcile"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/work"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/accelerator"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	modeldata "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/model"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/postgres"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/server"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/service"
)

// Name and Version can be overridden with -ldflags at build time.
var (
	Name     = "ani-inference-service"
	Version  = "dev"
	flagconf string
	id, _    = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "configs", "config path, for example -conf configs/config.yaml")
}

func main() {
	flag.Parse()
	logger := newRuntimeLogger(os.Stdout)
	log.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("service terminated", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	undoMaxProcs, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...interface{}) {
		log.Info("runtime CPU quota", "detail", fmt.Sprintf(format, args...))
	}))
	if err != nil {
		return fmt.Errorf("configure runtime CPU quota: %w", err)
	}
	defer undoMaxProcs()

	c := config.New(config.WithSource(
		file.NewSource(flagconf),
		env.NewSource("ANI"),
	))
	defer c.Close()
	if err := c.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	var bc inferencev1.Bootstrap
	if err := c.Scan(&bc); err != nil {
		return fmt.Errorf("scan config: %w", err)
	}
	var create service.CreateUseCase
	var read inferencebiz.ReadUseCase
	var command inferencebiz.CommandUseCase
	var update inferencebiz.UpdateUseCase
	var refund gpu.RefundReporter
	if strings.TrimSpace(os.Getenv("ANI_DATABASE_DSN")) == "" {
		return errors.New("ANI_DATABASE_DSN is required")
	}
	var pool *pgxpool.Pool
	var modelClient *modeldata.Client
	var background []kratosTransport.Server
	{
		dsn := os.Getenv("ANI_DATABASE_DSN")
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool, err = pgxpool.New(pingCtx, dsn)
		if err == nil {
			err = pool.Ping(pingCtx)
		}
		cancel()
		if err != nil {
			if pool != nil {
				pool.Close()
			}
			return fmt.Errorf("connect PostgreSQL: %w", err)
		}
		defer pool.Close()
		repo := postgres.NewRepository(pool)
		var closeRefund func()
		refund, closeRefund, err = configuredGovernanceRefundReporter(bc.GetManagedGpu(), repo)
		if err != nil {
			return err
		}
		defer closeRefund()
		create = postgres.NewCreateUseCase(repo)
		var closeModel func()
		var modelErr error
		modelClient, closeModel, modelErr = configuredModelClient()
		if modelErr != nil {
			return modelErr
		}
		defer closeModel()
		if modelClient != nil {
			create = modeldata.NewCreateUseCase(modelClient, create)
		}
		read = postgres.NewReadUseCase(pool)
		command = postgres.NewCommandUseCase(repo)
		update = postgres.NewUpdateUseCase(repo)
		if modelClient != nil {
			update = modeldata.NewUpdateUseCase(modelClient, update)
		}
		servers, err := configureKubernetesRuntime(pool, modelClient)
		if err != nil {
			return err
		}
		background = servers
	}
	app, err := buildAppWithAllDependenciesAndManagedGPU(&bc, logger, create, read, command, update, refund, background...)
	if err != nil {
		return fmt.Errorf("build app: %w", err)
	}
	if err := app.Run(); err != nil {
		return fmt.Errorf("run app: %w", err)
	}
	return nil
}

func configureKubernetesRuntime(pool *pgxpool.Pool, modelClient *modeldata.Client) ([]kratosTransport.Server, error) {
	return buildKubernetesServers(pool, modelClient)
}

func buildKubernetesServers(pool *pgxpool.Pool, modelClient *modeldata.Client) ([]kratosTransport.Server, error) {
	namespace := os.Getenv("ANI_INFERENCE_NAMESPACE")
	if namespace == "" {
		return nil, errors.New("ANI_INFERENCE_NAMESPACE is required for Kubernetes runtime")
	}
	publicBaseURL, err := requiredEnv("ANI_HIGRESS_PUBLIC_BASE_URL")
	if err != nil {
		return nil, err
	}
	config, err := inferenceRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes config: %w", err)
	}
	if modelClient == nil {
		return nil, errors.New("ANI_MODEL_GRPC_ADDR is required for Kubernetes runtime")
	}
	materializerSettings, err := configuredModelMaterializerSettings()
	if err != nil {
		return nil, err
	}
	workStore := postgres.NewWorkStore(pool, postgres.WorkStoreOptions{})
	reconcileStore := postgres.NewReconcileStore(pool)
	runtimeSource := postgres.NewRuntimeSource(pool, namespace)
	runtimeExecutor := &kubernetes.RuntimeExecutor{Source: runtimeSource, Bindings: postgres.NewRepository(pool), RequireQuota: false}
	kserveExecutor := &kubernetes.KServeRuntimeExecutor{RuntimeExecutor: runtimeExecutor}
	runtimeRouter := &kubernetes.RuntimeRouter{
		Source:              runtimeSource,
		Deployment:          runtimeExecutor,
		KServe:              kserveExecutor,
		DeploymentReconcile: runtimeExecutor,
		KServeReconcile:     kserveExecutor,
	}
	var runtimePort inferencebiz.RuntimePort = runtimeRouter
	var reconcileRuntime bizreconcile.Runtime = runtimeRouter
	domain := &bizreconcile.Reconciler{Repository: reconcileStore, Runtime: reconcileRuntime}
	controller := &kubernetes.Controller{Work: workStore}
	mgr, err := kubernetes.NewManager(config, controller, ctrlmanager.Options{Cache: ctrlcache.Options{DefaultNamespaces: map[string]ctrlcache.Config{namespace: {}}}})
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes manager: %w", err)
	}
	runtimeExecutor.Client = mgr.GetClient()
	runtimeExecutor.APIReader = mgr.GetAPIReader()
	runtimeExecutor.CRClient = mgr.GetClient()
	controller.Status = &kubernetes.StatusProjector{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Source: postgres.NewStatusProjectionSource(pool)}
	modelMaterializer := &modeldata.Materializer{
		Catalog:      modelClient,
		Source:       runtimeSource,
		Client:       mgr.GetClient(),
		Reader:       mgr.GetAPIReader(),
		StorageClass: materializerSettings.StorageClass,
		Image:        materializerSettings.Image,
		Namespace:    namespace,
	}
	publicationPublisher := &kubernetes.HTTPRoutePublisher{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Source:    runtimeSource,
		// Backend selection is read from each persisted runtime generation.
		// This compatibility field is intentionally empty so it cannot migrate
		// an existing service through process configuration.
		RuntimeProvider:  "",
		GatewayNamespace: envOrDefault("ANI_HIGRESS_GATEWAY_NAMESPACE", "higress-system"),
		GatewayName:      envOrDefault("ANI_HIGRESS_GATEWAY_NAME", "ani-higress"),
		RouteNamespace:   envOrDefault("ANI_HIGRESS_ROUTE_NAMESPACE", namespace),
		PublicBaseURL:    publicBaseURL,
		PathPrefix:       envOrDefault("ANI_HIGRESS_PATH_PREFIX", "/v1/completions"),
	}
	operationStore := postgres.NewOperationStore(pool)
	gpuResolver, err := configuredGPUResolver()
	if err != nil {
		return nil, err
	}
	operationRunner := &inferencebiz.Runner{
		Store:       operationStore,
		Admission:   &postgres.Admission{Source: runtimeSource},
		Model:       modelMaterializer,
		GPU:         gpuResolver,
		Publication: publicationPublisher,
		Runtime:     runtimePort,
		RetryAfter:  5 * time.Second,
	}
	loopServer, err := server.NewLoopServer(workStore, workStore, func(ctx context.Context, item work.Item) (work.Result, error) {
		return (&durableExecutor{Operations: operationRunner, Store: operationStore, Observation: domain}).Execute(ctx, item)
	}, 5*time.Second, nil)
	if err != nil {
		return nil, err
	}
	loopServer.WaitReady = func(ctx context.Context) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return errors.New("controller-runtime cache did not sync")
		}
		return nil
	}
	// Cache readiness alone is insufficient for business readiness. Keep
	// readyz false until every durable operation provider is explicitly wired;
	// the callback in newApp will promote readiness after this gate passes.
	loopServer.ReadyCheck = func() bool {
		return operationRunner.Admission != nil &&
			operationRunner.Model != nil &&
			operationRunner.Publication != nil &&
			operationRunner.Runtime != nil &&
			(operationRunner.Audit != nil || operationStoreSupportsAtomicAudit(operationRunner.Store))
	}
	return []kratosTransport.Server{loopServer, &server.ManagerServer{Manager: mgr}}, nil
}

// configuredGPUResolver is optional. An omitted endpoint leaves CPU-only
// generations independent of the accelerator service; the runner fails a
// generation that explicitly contains resource.gpu if no resolver is set.
func configuredGPUResolver() (inferencebiz.GPUResolver, error) {
	endpoint := strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_GRPC_ADDR"))
	if endpoint == "" {
		return nil, nil
	}
	clusterID := strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_CLUSTER_ID"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _, err := accelerator.Dial(ctx, accelerator.Config{
		Endpoint:  endpoint,
		ClusterID: clusterID,
		TLS: accelerator.TLSConfig{
			CAFile:     strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_CA_FILE")),
			CertFile:   strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_CERT_FILE")),
			KeyFile:    strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_KEY_FILE")),
			ServerName: strings.TrimSpace(os.Getenv("ANI_ACCELERATOR_SERVER_NAME")),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("configure accelerator resolver: %w", err)
	}
	return client, nil
}

func operationStoreSupportsAtomicAudit(store inferencebiz.OperationStore) bool {
	_, ok := store.(inferencebiz.AtomicStepStore)
	return ok
}

// durableExecutor selects the operation step runner while an operation is
// active and the continuous observation reconciler after it reaches a
// terminal phase. Both paths require the same resource_work lease.
type durableExecutor struct {
	Operations  *inferencebiz.Runner
	Store       inferencebiz.OperationStore
	Observation *bizreconcile.Reconciler
}

func (e *durableExecutor) Execute(ctx context.Context, item work.Item) (work.Result, error) {
	if e == nil || e.Operations == nil || e.Store == nil || e.Observation == nil {
		return work.Result{}, errors.New("durable executor is not configured")
	}
	op, err := e.Store.CurrentOperation(ctx, item)
	if err != nil {
		return work.Result{}, err
	}
	if op.Phase == inferencebiz.OperationPending || op.Phase == inferencebiz.OperationRunning {
		return e.Operations.Execute(ctx, item)
	}
	return e.Observation.Execute(ctx, item)
}

func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required for Kubernetes runtime", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func inferenceRESTConfig() (*rest.Config, error) {
	if path := os.Getenv("ANI_KUBECONFIG"); path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

func newRuntimeLogger(writer io.Writer) *slog.Logger {
	handler := log.NewHandler(
		log.WithWriter(writer),
		log.WithFormat(log.FormatJSON),
		log.WithLevel(log.LevelInfo),
		log.WithAddSource(true),
		log.WithExtractor(tracing.TraceAttrs),
		log.WithFilter(log.FilterKey(
			"args",
			"authorization",
			"cookie",
			"credential",
			"password",
			"private_key",
			"set-cookie",
			"token",
		)),
	)
	return slog.New(handler).With(
		slog.String("service.id", id),
		slog.String("service.name", Name),
		slog.String("service.version", Version),
	)
}
