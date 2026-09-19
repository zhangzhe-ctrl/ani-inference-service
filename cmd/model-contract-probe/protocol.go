package main

// Protocol-negative cases are deliberately confined to the test entrypoint.
// The production adapter has no identity override or insecure-TLS switch.
import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	modelv1 "github.com/zhangzhe-ctrl/ani-model-service/api/model/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func protocolCases(mode, address, caFile, certFile, keyFile, tenant, version string, timeout time.Duration) error {
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return fmt.Errorf("invalid CA")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "ani-model-service", RootCAs: roots}
	if mode != "no-cert" {
		cert, e := tls.LoadX509KeyPair(certFile, keyFile)
		if e != nil {
			return e
		}
		cfg.Certificates = []tls.Certificate{cert}
		if mode == "untrusted" {
			// Force presentation of the foreign-CA certificate so this is distinct from no-cert.
			cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil }
		}
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		return err
	}
	defer conn.Close()
	base := metadata.Pairs("x-ani-tenant-id", tenant, "x-ani-request-id", "99999999-9999-4999-8999-999999999999", "x-ani-actor", "governance:user:7")
	type testCase struct {
		name, method string
		md           metadata.MD
		req, reply   proto.Message
		want         codes.Code
	}
	get := "/model.v1.ModelService/GetModelVersion"
	list := "/model.v1.ModelService/ListModels"
	request := func(t, v string) *modelv1.GetModelVersionRequest {
		return &modelv1.GetModelVersionRequest{TenantId: t, ModelVersionId: v}
	}
	var cases []testCase
	addGet := func(name string, md metadata.MD, t, v string, want codes.Code) {
		cases = append(cases, testCase{name, get, md, request(t, v), new(modelv1.GetModelVersionResponse), want})
	}
	switch mode {
	case "no-cert", "untrusted", "wrong-service":
		addGet(mode, base, tenant, version, codes.Unavailable)
	case "governance":
		cases = append(cases, testCase{"governance-list", list, base, &modelv1.ListModelsRequest{TenantId: tenant}, new(modelv1.ListModelsResponse), codes.OK})
		addGet("governance-version-denied", base, tenant, version, codes.PermissionDenied)
	case "matrix":
		addGet("cross-tenant", base, tenant, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2", codes.NotFound)
		addGet("nonexistent", base, tenant, "cccccccc-cccc-4ccc-8ccc-cccccccccccc", codes.NotFound)
		addGet("tenant-mismatch", base, "22222222-2222-4222-8222-222222222222", version, codes.PermissionDenied)
		addGet("empty-request-tenant", base, "", version, codes.PermissionDenied)
		for _, key := range []string{"x-ani-tenant-id", "x-ani-request-id"} {
			for _, kind := range []string{"missing", "duplicate", "invalid", "zero", "noncanonical"} {
				md := base.Copy()
				switch kind {
				case "missing":
					md.Delete(key)
				case "duplicate":
					md.Append(key, md.Get(key)[0])
				case "invalid":
					md.Set(key, "bad")
				case "zero":
					md.Set(key, "00000000-0000-0000-0000-000000000000")
				case "noncanonical":
					md.Set(key, "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA")
				}
				addGet(key+"-"+kind, md, tenant, version, codes.Unauthenticated)
			}
		}
		addGet("forged-actor-read", base, tenant, version, codes.OK)
		cases = append(cases, testCase{"inference-list-denied", list, base, &modelv1.ListModelsRequest{TenantId: tenant}, new(modelv1.ListModelsResponse), codes.PermissionDenied})
		cases = append(cases, testCase{"inference-write-denied", "/model.v1.ModelService/DeleteModel", base, &modelv1.DeleteModelRequest{TenantId: tenant, ModelId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"}, new(modelv1.ListModelsResponse), codes.PermissionDenied})
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
	for _, tc := range cases {
		ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), tc.md), timeout)
		err = conn.Invoke(ctx, tc.method, tc.req, tc.reply)
		cancel()
		if status.Code(err) != tc.want {
			return fmt.Errorf("FAIL %s code=%s want=%s err=%v", tc.name, status.Code(err), tc.want, err)
		}
		if err != nil && proto.Size(tc.reply) != 0 {
			return fmt.Errorf("FAIL %s leaked response", tc.name)
		}
		if tc.name == "governance-list" {
			out := tc.reply.(*modelv1.ListModelsResponse)
			if len(out.Models) != 1 || out.Models[0].TenantId != tenant {
				return fmt.Errorf("governance list lost tenant data: %v", out)
			}
		}
		if tc.name == "forged-actor-read" && tc.reply.(*modelv1.GetModelVersionResponse).GetVersion().GetId() != version {
			return fmt.Errorf("forged actor read lost version")
		}
		fmt.Printf("PASS %s code=%s\n", tc.name, status.Code(err))
	}
	return nil
}

// verifySerial is additional rotation evidence, never a replacement for adapter reads.
func verifySerial(address, caFile, certFile, keyFile, expected string, timeout time.Duration) error {
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return fmt.Errorf("invalid CA")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "ani-model-service", Certificates: []tls.Certificate{cert}}}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	state := conn.(*tls.Conn).ConnectionState()
	serial := state.PeerCertificates[0].SerialNumber.Text(16)
	if !strings.EqualFold(serial, strings.TrimLeft(expected, "0")) {
		return fmt.Errorf("server serial=%s want=%s", serial, expected)
	}
	client, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	fmt.Printf("PASS handshake server_serial=%s client_serial=%s TLS=%x\n", serial, client.SerialNumber.Text(16), state.Version)
	return nil
}
