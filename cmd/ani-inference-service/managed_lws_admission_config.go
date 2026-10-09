package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	kratosTransport "github.com/go-kratos/kratos/v3/transport"
	inferencev1 "github.com/zhangzhe-ctrl/ani-inference-service/api/inference/v1"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/data/kubernetes"
	"github.com/zhangzhe-ctrl/ani-inference-service/internal/server"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

type managedLWSAdmissionSettings struct {
	Host                   string
	Port                   int
	CAFile                 string
	CertFile               string
	KeyFile                string
	APIServerClientDNSName string
	ControllerUsername     string
}

func managedLWSAdmissionConfig(managed *inferencev1.ManagedGPU) (managedLWSAdmissionSettings, bool, error) {
	_, managedEnabled, err := governanceRefundConfig(managed)
	if err != nil {
		return managedLWSAdmissionSettings{}, managedEnabled, err
	}
	config := managed.GetLwsAdmission()
	address := strings.TrimSpace(config.GetAddress())
	settings := managedLWSAdmissionSettings{
		CAFile:                 strings.TrimSpace(config.GetCaFile()),
		CertFile:               strings.TrimSpace(config.GetCertFile()),
		KeyFile:                strings.TrimSpace(config.GetKeyFile()),
		APIServerClientDNSName: strings.TrimSpace(config.GetApiServerClientDnsName()),
		ControllerUsername:     strings.TrimSpace(config.GetControllerUsername()),
	}
	enabled := managedEnabled || address != "" || settings.CAFile != "" || settings.CertFile != "" || settings.KeyFile != "" || settings.APIServerClientDNSName != "" || settings.ControllerUsername != ""
	if !enabled {
		return settings, false, nil
	}
	if address == "" || settings.CAFile == "" || settings.CertFile == "" || settings.KeyFile == "" || settings.APIServerClientDNSName == "" || settings.ControllerUsername == "" {
		return settings, true, errors.New("managed_gpu.lws_admission requires address, CA, certificate, key, API server client DNS name and controller username")
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return settings, true, errors.New("managed_gpu.lws_admission.address requires an explicit host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return settings, true, errors.New("managed_gpu.lws_admission.address requires a numeric port in 1..65535")
	}
	if net.ParseIP(settings.APIServerClientDNSName) != nil || len(validation.IsDNS1123Subdomain(settings.APIServerClientDNSName)) != 0 {
		return settings, true, errors.New("managed_gpu.lws_admission.api_server_client_dns_name requires an exact DNS name")
	}
	username := strings.Split(settings.ControllerUsername, ":")
	if len(username) != 4 || username[0] != "system" || username[1] != "serviceaccount" || len(validation.IsDNS1123Label(username[2])) != 0 || len(validation.IsDNS1123Subdomain(username[3])) != 0 {
		return settings, true, errors.New("managed_gpu.lws_admission.controller_username requires an exact Kubernetes service-account username")
	}
	settings.Host, settings.Port = host, port
	return settings, true, nil
}

func configuredManagedLWSAdmissionServer(managed *inferencev1.ManagedGPU, namespace string) (webhook.Server, kubernetes.ManagedLWSAdmissionConfig, error) {
	settings, enabled, err := managedLWSAdmissionConfig(managed)
	if err != nil || !enabled {
		return nil, kubernetes.ManagedLWSAdmissionConfig{}, err
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, kubernetes.ManagedLWSAdmissionConfig{}, errors.New("managed LWS admission requires the Inference namespace")
	}
	tlsConfig, err := managedLWSAdmissionTLS(settings)
	if err != nil {
		return nil, kubernetes.ManagedLWSAdmissionConfig{}, err
	}
	server := webhook.NewServer(webhook.Options{
		Host: settings.Host,
		Port: settings.Port,
		TLSOpts: []func(*tls.Config){func(config *tls.Config) {
			config.MinVersion = tlsConfig.MinVersion
			config.ClientAuth = tlsConfig.ClientAuth
			config.ClientCAs = tlsConfig.ClientCAs
			config.GetCertificate = tlsConfig.GetCertificate
			config.VerifyConnection = tlsConfig.VerifyConnection
		}},
	})
	return server, kubernetes.ManagedLWSAdmissionConfig{
		Namespace:              namespace,
		APIServerClientDNSName: settings.APIServerClientDNSName,
		ControllerUsername:     settings.ControllerUsername,
	}, nil
}

func managedLWSAdmissionTLS(settings managedLWSAdmissionSettings) (*tls.Config, error) {
	caPEM, err := os.ReadFile(settings.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read managed LWS admission client CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("managed LWS admission client CA bundle has no certificates")
	}
	identity, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load managed LWS admission server identity: %w", err)
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS13,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      roots,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &identity, nil },
		VerifyConnection: func(state tls.ConnectionState) error {
			if state.Version < tls.VersionTLS13 || len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return errors.New("verified API server client identity is required")
			}
			leaf := state.PeerCertificates[0]
			if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != settings.APIServerClientDNSName || len(leaf.URIs) != 0 || len(leaf.IPAddresses) != 0 || len(leaf.EmailAddresses) != 0 {
				return errors.New("unregistered API server client identity")
			}
			return nil
		},
	}, nil
}

// Only production composition that registered the real handler sets this
// marker. It retains ManagerServer's lifecycle and reports no business readiness.
type managedLWSManagerServer struct {
	*server.ManagerServer
	admissionRegistered bool
}

func (s *managedLWSManagerServer) ManagedLWSAdmissionConfigured() bool {
	return s != nil && s.ManagerServer != nil && s.Manager != nil && s.admissionRegistered
}

func requireManagedLWSAdmissionComposition(managed *inferencev1.ManagedGPU, background []kratosTransport.Server) error {
	_, enabled, err := governanceRefundConfig(managed)
	if err != nil || !enabled {
		return err
	}
	if _, _, err := managedLWSAdmissionConfig(managed); err != nil {
		return err
	}
	for _, candidate := range background {
		if configured, ok := candidate.(interface{ ManagedLWSAdmissionConfigured() bool }); ok && configured.ManagedLWSAdmissionConfigured() {
			return nil
		}
	}
	return errors.New("managed GPU receiver requires the registered synchronous LWS admission manager")
}
