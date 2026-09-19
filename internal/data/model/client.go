// Package model implements the authenticated Model version-read adapter.
package model

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	modelv1 "github.com/zhangzhe-ctrl/ani-model-service/api/model/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	conn    *grpc.ClientConn
	rpc     modelv1.ModelServiceClient
	timeout time.Duration
}

// New loads credentials once; rotate them with a new process/Client.
func New(address, caFile, certFile, keyFile string, timeout time.Duration) (*Client, error) {
	if address == "" || timeout <= 0 {
		return nil, fmt.Errorf("model address and positive timeout required")
	}
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read model CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("model CA contains no certificates")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load inference certificate: %w", err)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "ani-model-service", Certificates: []tls.Certificate{cert},
	})))
	if err != nil {
		return nil, fmt.Errorf("create model connection: %w", err)
	}
	return &Client{conn: conn, rpc: modelv1.NewModelServiceClient(conn), timeout: timeout}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// GetModelVersion trusts the calling Inference use case to authorize the tenant.
// Never forward caller-supplied identity metadata to Model.
func (c *Client) GetModelVersion(ctx context.Context, tenant, versionID string) (*modelv1.GetModelVersionResponse, error) {
	id, err := uuid.Parse(tenant)
	if err != nil || id == uuid.Nil || id.String() != tenant {
		return nil, fmt.Errorf("canonical nonzero tenant UUID required")
	}
	if versionID == "" {
		return nil, fmt.Errorf("model version ID required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-ani-tenant-id", tenant, "x-ani-request-id", uuid.NewString()))
	out, err := c.rpc.GetModelVersion(ctx, &modelv1.GetModelVersionRequest{TenantId: tenant, ModelVersionId: versionID})
	if err != nil {
		return nil, fmt.Errorf("get model version: %w", err)
	}
	return out, nil
}
