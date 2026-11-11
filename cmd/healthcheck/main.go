// Command healthcheck performs one standard gRPC health check against the
// dispatch server and exits 0 only if the service reports SERVING. The stack
// script uses it to wait for real readiness rather than an open port.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "dispatch gRPC address")
	service := flag.String("service", "dispatch.v1.DispatchService", "health service name")
	timeout := flag.Duration("timeout", 2*time.Second, "deadline for the check")
	flag.Parse()

	if err := check(*addr, *service, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck %s: %v\n", *addr, err)
		os.Exit(1)
	}
}

func check(addr, service string, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
	if err != nil {
		return err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("service %q is %s", service, resp.GetStatus())
	}
	return nil
}
