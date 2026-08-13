// Command route95_grpc_health checks one or more plaintext internal gRPC
// endpoints. It is an acceptance utility and does not send business traffic.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: route95_grpc_health host:port [...]")
		os.Exit(2)
	}
	for _, target := range os.Args[1:] {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err == nil {
			response, checkErr := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			if checkErr != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
				err = fmt.Errorf("status=%s error=%v", response.GetStatus(), checkErr)
			}
			_ = conn.Close()
		}
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s FAIL: %v\n", target, err)
			os.Exit(1)
		}
		fmt.Printf("%s SERVING\n", target)
	}
}
