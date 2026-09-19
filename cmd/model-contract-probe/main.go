// model-contract-probe assembles the production adapter without the Inference runtime.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	adapter "github.com/zhangzhe-ctrl/ani-inference-service/internal/data/model"
	modelv1 "github.com/zhangzhe-ctrl/ani-model-service/api/model/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	address := flag.String("address", "model:19090", "Model gRPC address")
	ca := flag.String("ca", "/trust/ca.crt", "public trust bundle")
	cert := flag.String("cert", "/identity/tls.crt", "client certificate")
	key := flag.String("key", "/identity/tls.key", "client private key")
	tenant := flag.String("tenant", "", "controlled tenant UUID")
	version := flag.String("version", "", "model version UUID")
	expected := flag.String("expected", "", "expected protobuf JSON file, exported from PostgreSQL")
	mode := flag.String("mode", "read", "read, matrix, governance, no-cert, untrusted, wrong-service, outage")
	timeout := flag.Duration("timeout", 3*time.Second, "per-call timeout")
	serial := flag.String("server-serial", "", "optional expected server certificate serial for rotation evidence")
	flag.Parse()
	if *serial != "" {
		if err := verifySerial(*address, *ca, *cert, *key, *serial, *timeout); err != nil {
			return err
		}
	}
	if *mode != "read" && *mode != "outage" {
		return protocolCases(*mode, *address, *ca, *cert, *key, *tenant, *version, *timeout)
	}
	client, err := adapter.New(*address, *ca, *cert, *key, *timeout)
	if err != nil {
		return err
	}
	defer client.Close()
	started := time.Now()
	out, err := client.GetModelVersion(context.Background(), *tenant, *version)
	if *mode == "outage" {
		if (status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded) || out != nil || time.Since(started) > *timeout+time.Second {
			return fmt.Errorf("outage did not fail within budget: %v", err)
		}
		fmt.Printf("PASS outage elapsed=%s error=%v\n", time.Since(started), err)
		return nil
	}
	if err != nil {
		return err
	}
	if *expected == "" {
		return fmt.Errorf("read requires PostgreSQL expected response")
	}
	data, err := os.ReadFile(*expected)
	if err != nil {
		return err
	}
	want := new(modelv1.GetModelVersionResponse)
	if err = protojson.Unmarshal(data, want); err != nil {
		return err
	}
	if !proto.Equal(out, want) {
		return fmt.Errorf("response differs from PostgreSQL: got=%s want=%s", protojson.Format(out), protojson.Format(want))
	}
	fmt.Printf("PASS read elapsed=%s response=%s\n", time.Since(started), protojson.Format(out))
	return nil
}
