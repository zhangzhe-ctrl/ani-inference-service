package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kratosTransport "github.com/go-kratos/kratos/v3/transport"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/biz/gpu"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/server"
	"google.golang.org/protobuf/types/known/durationpb"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

func managedAdmissionConfigFixture() *inferencev1.ManagedGPU {
	config := validManagedConfig()
	config.LwsAdmission = &inferencev1.ManagedGPU_LWSAdmission{
		Address:                "127.0.0.1:19443",
		CaFile:                 "client-ca.pem",
		CertFile:               "webhook.pem",
		KeyFile:                "webhook-key.pem",
		ApiServerClientDnsName: "apiserver.test.internal",
		ControllerUsername:     "system:serviceaccount:kserve-test:kserve-controller",
	}
	return config
}

func TestManagedLWSAdmissionRequiresExplicitProductionConfiguration(t *testing.T) {
	for _, config := range []*inferencev1.ManagedGPU{nil, {}, {LwsAdmission: &inferencev1.ManagedGPU_LWSAdmission{}}} {
		if _, enabled, err := managedLWSAdmissionConfig(config); err != nil || enabled {
			t.Fatalf("CPU configuration: enabled=%v err=%v", enabled, err)
		}
	}
	if _, enabled, err := managedLWSAdmissionConfig(validManagedConfig()); !enabled || err == nil {
		t.Fatalf("managed receiver without admission: enabled=%v err=%v", enabled, err)
	}
	settings, enabled, err := managedLWSAdmissionConfig(managedAdmissionConfigFixture())
	if err != nil || !enabled || settings.Host != "127.0.0.1" || settings.Port != 19443 || settings.APIServerClientDNSName != "apiserver.test.internal" || settings.ControllerUsername != "system:serviceaccount:kserve-test:kserve-controller" {
		t.Fatalf("explicit admission configuration: settings=%+v enabled=%v err=%v", settings, enabled, err)
	}
	for _, test := range []struct {
		name  string
		alter func(*inferencev1.ManagedGPU_LWSAdmission)
	}{
		{"missing-address", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.Address = "" }},
		{"missing-client-ca", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.CaFile = "" }},
		{"missing-server-cert", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.CertFile = "" }},
		{"missing-server-key", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.KeyFile = "" }},
		{"missing-api-server-identity", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.ApiServerClientDnsName = "" }},
		{"missing-controller-identity", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.ControllerUsername = "" }},
		{"implicit-host", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.Address = ":19443" }},
		{"implicit-port", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.Address = "127.0.0.1:0" }},
		{"invalid-port", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.Address = "127.0.0.1:65536" }},
		{"named-port", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.Address = "127.0.0.1:https" }},
		{"wildcard-api-server", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.ApiServerClientDnsName = "*.test.internal" }},
		{"ip-api-server", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.ApiServerClientDnsName = "127.0.0.1" }},
		{"non-service-account-controller", func(c *inferencev1.ManagedGPU_LWSAdmission) { c.ControllerUsername = "kserve-controller" }},
		{"wildcard-controller", func(c *inferencev1.ManagedGPU_LWSAdmission) {
			c.ControllerUsername = "system:serviceaccount:kserve-test:*"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := managedAdmissionConfigFixture()
			test.alter(config.LwsAdmission)
			if _, _, err := managedLWSAdmissionConfig(config); err == nil {
				t.Fatal("incomplete or ambiguous production identity accepted")
			}
		})
	}
}

func TestManagedLWSAdmissionUsesExplicitMTLSAndExactAPIServerIdentity(t *testing.T) {
	config := managedAdmissionConfigFixture()
	config.LwsAdmission.CaFile, config.LwsAdmission.CertFile, config.LwsAdmission.KeyFile = managedAdmissionTestFiles(t)
	configured, identity, err := configuredManagedLWSAdmissionServer(config, "inference-test")
	if err != nil {
		t.Fatal(err)
	}
	if identity.Namespace != "inference-test" || identity.APIServerClientDNSName != config.LwsAdmission.ApiServerClientDnsName || identity.ControllerUsername != config.LwsAdmission.ControllerUsername {
		t.Fatalf("admission identity was changed: %+v", identity)
	}
	actual, ok := configured.(*webhook.DefaultServer)
	if !ok || actual.Options.Host != "127.0.0.1" || actual.Options.Port != 19443 {
		t.Fatalf("webhook listener configuration=%v", configured)
	}
	tlsConfig := &tls.Config{}
	for _, option := range actual.Options.TLSOpts {
		option(tlsConfig)
	}
	if tlsConfig.MinVersion != tls.VersionTLS13 || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil || tlsConfig.GetCertificate == nil || tlsConfig.VerifyConnection == nil {
		t.Fatalf("webhook TLS requirements incomplete: %+v", tlsConfig)
	}
	if certificate, err := tlsConfig.GetCertificate(nil); err != nil || certificate == nil || len(certificate.Certificate) == 0 {
		t.Fatalf("explicit serving certificate missing: %v", err)
	}
	trusted := &x509.Certificate{DNSNames: []string{config.LwsAdmission.ApiServerClientDnsName}}
	verified := tls.ConnectionState{Version: tls.VersionTLS13, PeerCertificates: []*x509.Certificate{trusted}, VerifiedChains: [][]*x509.Certificate{{trusted}}}
	if err := tlsConfig.VerifyConnection(verified); err != nil {
		t.Fatalf("configured API server identity rejected: %v", err)
	}
	for _, test := range []struct {
		name  string
		alter func(*tls.ConnectionState)
	}{
		{"tls12", func(s *tls.ConnectionState) { s.Version = tls.VersionTLS12 }},
		{"unverified", func(s *tls.ConnectionState) { s.VerifiedChains = nil }},
		{"no-certificate", func(s *tls.ConnectionState) { s.PeerCertificates = nil }},
		{"wrong-dns", func(s *tls.ConnectionState) {
			s.PeerCertificates = []*x509.Certificate{{DNSNames: []string{"other.test.internal"}}}
		}},
		{"multiple-dns", func(s *tls.ConnectionState) {
			s.PeerCertificates = []*x509.Certificate{{DNSNames: []string{config.LwsAdmission.ApiServerClientDnsName, "other.test.internal"}}}
		}},
		{"extra-ip", func(s *tls.ConnectionState) {
			s.PeerCertificates = []*x509.Certificate{{DNSNames: trusted.DNSNames, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}}
		}},
		{"extra-uri", func(s *tls.ConnectionState) {
			s.PeerCertificates = []*x509.Certificate{{DNSNames: trusted.DNSNames, URIs: []*url.URL{{Scheme: "spiffe", Host: "other.test.internal"}}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := verified
			test.alter(&state)
			if err := tlsConfig.VerifyConnection(state); err == nil {
				t.Fatal("unverified or ambiguous API server identity accepted")
			}
		})
	}
	if _, _, err := configuredManagedLWSAdmissionServer(config, ""); err == nil {
		t.Fatal("admission outside a configured Inference namespace accepted")
	}
	config.LwsAdmission.CaFile = filepath.Join(t.TempDir(), "missing-ca.pem")
	if _, _, err := configuredManagedLWSAdmissionServer(config, "inference-test"); err == nil {
		t.Fatal("unreadable client CA silently accepted")
	}
	if err := os.WriteFile(config.LwsAdmission.CaFile, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := configuredManagedLWSAdmissionServer(config, "inference-test"); err == nil {
		t.Fatal("invalid client CA silently accepted")
	}
	config.LwsAdmission.CaFile, config.LwsAdmission.CertFile, config.LwsAdmission.KeyFile = managedAdmissionTestFiles(t)
	_, _, otherKey := managedAdmissionTestFiles(t)
	config.LwsAdmission.KeyFile = otherKey
	if _, _, err := configuredManagedLWSAdmissionServer(config, "inference-test"); err == nil {
		t.Fatal("mismatched serving certificate and key silently accepted")
	}
}

type managedAdmissionManagerRunner struct{}

func (managedAdmissionManagerRunner) Start(context.Context) error { return nil }

func TestManagedLWSAdmissionCompositionRequiresRegisteredManager(t *testing.T) {
	config := managedAdmissionConfigFixture()
	if err := requireManagedLWSAdmissionComposition(nil, nil); err != nil {
		t.Fatalf("CPU-only composition rejected: %v", err)
	}
	manager := &managedLWSManagerServer{ManagerServer: &server.ManagerServer{Manager: managedAdmissionManagerRunner{}}}
	for _, background := range [][]kratosTransport.Server{nil, {manager}, {&server.ManagerServer{Manager: managedAdmissionManagerRunner{}}}} {
		if err := requireManagedLWSAdmissionComposition(config, background); err == nil {
			t.Fatal("managed receiver accepted an absent or unregistered admission manager")
		}
	}
	manager.admissionRegistered = true
	if err := requireManagedLWSAdmissionComposition(config, []kratosTransport.Server{manager}); err != nil {
		t.Fatalf("registered admission composition rejected: %v", err)
	}
	manager.Manager = nil
	if err := requireManagedLWSAdmissionComposition(config, []kratosTransport.Server{manager}); err == nil {
		t.Fatal("registered marker without a manager accepted")
	}
}

type managedAdmissionUnusedRefund struct{}

func (managedAdmissionUnusedRefund) ReportQuotaRelease(context.Context, gpu.ReleaseNotification) (gpu.ReleaseReceipt, error) {
	return gpu.ReleaseReceipt{}, errors.New("startup guard must not invoke the refund reporter")
}

func TestManagedReceiverAppRefusesMissingSynchronousLWSAdmission(t *testing.T) {
	config := &inferencev1.Bootstrap{
		Server: &inferencev1.Server{
			Grpc:            &inferencev1.Server_GRPC{Network: "tcp", Addr: "127.0.0.1:19090", Timeout: durationpb.New(time.Second)},
			Admin:           &inferencev1.Server_Admin{Network: "tcp", Addr: "127.0.0.1:19091", Timeout: durationpb.New(time.Second)},
			ShutdownTimeout: durationpb.New(5 * time.Second),
		},
		ManagedGpu: managedAdmissionConfigFixture(),
	}
	app, err := buildAppWithAllDependenciesAndManagedGPU(config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil, nil, nil, managedAdmissionUnusedRefund{})
	if app != nil || err == nil || !strings.Contains(err.Error(), "registered synchronous LWS admission manager") {
		t.Fatalf("app without admission registration: app=%v err=%v", app, err)
	}
}

func managedAdmissionTestFiles(t *testing.T) (string, string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"webhook.test.internal"},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	ca, cert, key := filepath.Join(directory, "ca.pem"), filepath.Join(directory, "cert.pem"), filepath.Join(directory, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	for path, data := range map[string][]byte{ca: certPEM, cert: certPEM, key: keyPEM} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return ca, cert, key
}
