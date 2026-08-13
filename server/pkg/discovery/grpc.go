package discovery

import (
	"context"

	"google.golang.org/grpc/resolver"
)

// GRPCBuilder adapts the etcd Resolver to gRPC's resolver API. The caller uses
// target "farm-etcd:///gamesvr" and round_robin, so each HTTP/2 SubConn tracks
// the same live instance set as the legacy HTTP transport.
type GRPCBuilder struct {
	SchemeName string
	Resolver   *Resolver
}

func (b *GRPCBuilder) Scheme() string { return b.SchemeName }

func (b *GRPCBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &grpcResolver{cancel: cancel}
	update := func() {
		endpoints := b.Resolver.Endpoints()
		addresses := make([]resolver.Address, 0, len(endpoints))
		for _, endpoint := range endpoints {
			if endpoint.GRPCAddr != "" {
				addresses = append(addresses, resolver.Address{Addr: endpoint.GRPCAddr, ServerName: endpoint.InstanceID})
			}
		}
		_ = cc.UpdateState(resolver.State{Addresses: addresses})
	}
	update()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-b.Resolver.Changed():
				update()
			}
		}
	}()
	return r, nil
}

type grpcResolver struct{ cancel context.CancelFunc }

func (*grpcResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *grpcResolver) Close()                              { r.cancel() }
