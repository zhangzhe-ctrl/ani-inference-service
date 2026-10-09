// Package governance implements the internal owner-to-Governance quota refund
// connection. It does not schedule notifications or determine cleanup success.
package governance

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	quotav1 "github.com/zhangzhe-ctrl/ani-governance/api/quota/gen/go/quota/service/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var ErrNotConfigured = errors.New("Governance refund reporter is not configured")

type TLSConfig struct{ CAFile, CertFile, KeyFile, ServerName string }
type Config struct {
	Endpoint string
	TLS      TLSConfig
	Timeout  time.Duration
}

func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.Endpoint) == "" || strings.TrimSpace(cfg.TLS.CAFile) == "" || strings.TrimSpace(cfg.TLS.CertFile) == "" || strings.TrimSpace(cfg.TLS.KeyFile) == "" || strings.TrimSpace(cfg.TLS.ServerName) == "" || cfg.Timeout <= 0 {
		return ErrNotConfigured
	}
	return nil
}

type Client struct {
	release  quotav1.QuotaReleaseServiceClient
	contexts gpu.RefundContextStore
	timeout  time.Duration
}

var _ gpu.RefundReporter = (*Client)(nil)

func NewClient(release quotav1.QuotaReleaseServiceClient, contexts gpu.RefundContextStore, timeout time.Duration) (*Client, error) {
	if release == nil || contexts == nil || timeout <= 0 {
		return nil, ErrNotConfigured
	}
	return &Client{release: release, contexts: contexts, timeout: timeout}, nil
}

// Dial owns a TLS 1.3 gRPC connection. The process composition root closes it.
func Dial(ctx context.Context, cfg Config, contexts gpu.RefundContextStore) (*Client, func() error, error) {
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}
	if contexts == nil {
		return nil, nil, ErrNotConfigured
	}
	ca, err := os.ReadFile(cfg.TLS.CAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read Governance CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, nil, errors.New("Governance CA bundle has no certificates")
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load Inference refund identity: %w", err)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: cfg.TLS.ServerName}
	dialCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, cfg.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithBlock())
	if err != nil {
		return nil, nil, fmt.Errorf("dial Governance refund service: %w", err)
	}
	client, err := NewClient(quotav1.NewQuotaReleaseServiceClient(conn), contexts, cfg.Timeout)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return client, conn.Close, nil
}

func (c *Client) ReportQuotaRelease(ctx context.Context, n gpu.ReleaseNotification) (gpu.ReleaseReceipt, error) {
	if c == nil || c.release == nil || c.contexts == nil || c.timeout <= 0 {
		return gpu.ReleaseReceipt{}, ErrNotConfigured
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	original, err := c.contexts.LoadRefundContext(callCtx, n.TenantID, n.ResourceID, n.OriginalCreateOperationID)
	if err != nil {
		return gpu.ReleaseReceipt{}, fmt.Errorf("load original refund context: %w", err)
	}
	if err := gpu.ValidateRefund(n, original); err != nil {
		return gpu.ReleaseReceipt{}, err
	}
	req := &quotav1.ReportQuotaReleaseRequest{ReleaseEventId: n.ReleaseEventID, OperationId: n.OriginalCreateOperationID}
	switch n.Reason {
	case gpu.ResourceReleased:
		req.Reason = quotav1.ReleaseReason_RESOURCE_RELEASED
	case gpu.AbortedCleaned:
		req.Reason = quotav1.ReleaseReason_ABORTED_CLEANED
	}
	for _, it := range n.Items {
		req.Items = append(req.Items, &quotav1.QuotaReleaseItem{ChargeId: it.ChargeID, QuotaCode: it.QuotaCode, ReleasedTotal: it.ReleasedTotal})
	}
	for _, ref := range n.ResourceRefs {
		req.ResourceRefs = append(req.ResourceRefs, &quotav1.QuotaResourceRef{ResourceId: ref})
	}
	response, err := c.release.ReportQuotaRelease(callCtx, req)
	if err != nil {
		return gpu.ReleaseReceipt{}, err
	}
	if response == nil {
		return gpu.ReleaseReceipt{}, fmt.Errorf("%w: missing Governance receipt", gpu.ErrInvalidRefund)
	}
	receipt := gpu.ReleaseReceipt{Items: make([]gpu.ReleaseResult, 0, len(response.Items))}
	for _, result := range response.Items {
		if result == nil {
			return gpu.ReleaseReceipt{}, fmt.Errorf("%w: empty Governance receipt item", gpu.ErrInvalidRefund)
		}
		receipt.Items = append(receipt.Items, gpu.ReleaseResult{ChargeID: result.ChargeId, AppliedDelta: result.AppliedDelta, ReleasedTotal: result.ReleasedTotal})
	}
	if err := gpu.ValidateReleaseReceipt(n, original, receipt); err != nil {
		return gpu.ReleaseReceipt{}, err
	}
	return receipt, nil
}
