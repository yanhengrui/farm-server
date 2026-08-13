package routing

import (
	"context"
	"fmt"
	"sync"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
)

type RouteResolver interface {
	Resolve(farmID int64) (Route, error)
}

// Client resolves every command by farm_id, stamps the owner epoch, and then
// uses the route 9.3 dual-stack transport for that exact owner. There is no
// random fallback when a bucket has no owner.
type Client struct {
	resolver RouteResolver
	mode     rpcgrpc.Mode
	grpcCfg  rpcgrpc.ClientConfig
	mu       sync.Mutex
	clients  map[string]*farmsvc.Client
	conns    map[string]*grpc.ClientConn
}

func NewClient(resolver RouteResolver, mode rpcgrpc.Mode, grpcCfg rpcgrpc.ClientConfig) *Client {
	return &Client{resolver: resolver, mode: mode, grpcCfg: grpcCfg, clients: make(map[string]*farmsvc.Client), conns: make(map[string]*grpc.ClientConn)}
}

func (c *Client) SubmitCommand(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	route, err := c.resolver.Resolve(cmd.FarmID)
	if err != nil {
		return application.CommitResult{}, err
	}
	cmd.RouteEpoch = route.Epoch
	client, err := c.clientFor(route)
	if err != nil {
		return application.CommitResult{}, err
	}
	return client.SubmitCommand(ctx, cmd)
}

func (c *Client) clientFor(route Route) (*farmsvc.Client, error) {
	key := fmt.Sprintf("%s|%s|%s|%s", route.Owner.InstanceID, route.Owner.HTTPAddr, route.Owner.GRPCAddr, c.mode)
	c.mu.Lock()
	defer c.mu.Unlock()
	if client := c.clients[key]; client != nil {
		return client, nil
	}
	if route.Owner.HTTPAddr == "" {
		return nil, fmt.Errorf("route owner %s has no HTTP address", route.Owner.InstanceID)
	}
	client := farmsvc.NewClient(route.Owner.HTTPAddr)
	if c.mode != rpcgrpc.ModeHTTP {
		if route.Owner.GRPCAddr == "" {
			return nil, fmt.Errorf("route owner %s has no gRPC address", route.Owner.InstanceID)
		}
		conn, err := rpcgrpc.Dial(route.Owner.GRPCAddr, c.grpcCfg)
		if err != nil {
			return nil, err
		}
		c.conns[key] = conn
		client.WithGRPC(rpcv1.NewFarmCommandServiceClient(conn), c.mode)
	}
	c.clients[key] = client
	return client, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for key, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
		delete(c.conns, key)
	}
	clear(c.clients)
	return first
}
