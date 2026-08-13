// farmsvr：Farm Router + Farm Actor 逻辑串行、预校验、热快照、patch/广播编排。
// 不直写 MySQL 业务事实；通过 CommitFarmCommand 调 gamesvr 提交（见 ADR-017）。
// 同时向 gatesvr 暴露 /farm/submit HTTP/JSON 端点（farmsvc transport）。
// 本 main 只做依赖装配与启动。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/actor"
	"github.com/photon/farm-server/server/internal/farm/realtime"
	"github.com/photon/farm-server/server/internal/farm/routing"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	"github.com/photon/farm-server/server/pkg/app"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/config"
	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/logging"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/probe"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg, err := config.Load(false) // farmsvr 不直连 MySQL（经 gamesvr 提交）
	if err != nil {
		panic(err)
	}
	log := logging.New(cfg.ServiceName, cfg.InstanceID, cfg.AppEnv, cfg.LogLevel)
	metrics := observability.New(cfg.ServiceName, log)
	http.DefaultTransport = metrics.Transport(cfg.ServiceName, http.DefaultTransport)
	controlCtx, controlCancel := context.WithCancel(context.Background())

	mode := rpcgrpc.ParseMode(cfg.RPCTransport)
	clientCfg := rpcgrpc.ClientConfig{DefaultTimeout: cfg.RPCDefaultTimeout, KeepaliveTime: cfg.RPCKeepaliveTime, KeepaliveTimeout: cfg.RPCKeepaliveTimeout, MaxMessageBytes: cfg.RPCMaxMessageBytes, UnaryInterceptors: []grpc.UnaryClientInterceptor{metrics.GRPCClientInterceptor(cfg.ServiceName)}}
	var gameConn *grpc.ClientConn
	var etcdClient *clientv3.Client
	var routeController *routing.Controller
	var gameResolver *discovery.Resolver
	gameHTTPAddr, gameGRPCTarget := cfg.GameSvrAddr, cfg.GameSvrGRPCAddr
	if cfg.EtcdEndpoints != "" {
		etcdClient, err = discovery.NewEtcdClient(cfg.EtcdEndpoints, cfg.EtcdUsername, cfg.EtcdPassword)
		if err != nil {
			panic(err)
		}
		gameResolver = discovery.NewResolver(etcdClient, cfg.EtcdPrefix, "gamesvr")
		if err = gameResolver.Start(controlCtx); err != nil {
			panic(err)
		}
		http.DefaultTransport = &discovery.HTTPTransport{Base: http.DefaultTransport, LogicalHost: "gamesvr.internal", Resolver: gameResolver}
		gameHTTPAddr = "http://gamesvr.internal"
		if mode != rpcgrpc.ModeHTTP {
			builder := &discovery.GRPCBuilder{SchemeName: "farm-gamesvr", Resolver: gameResolver}
			clientCfg.Resolver, clientCfg.LoadBalancing = builder, "round_robin"
			gameGRPCTarget = builder.Scheme() + ":///gamesvr"
		}
		routeController = routing.NewController(etcdClient, cfg.EtcdPrefix, cfg.RouteBucketCount, discovery.Endpoint{InstanceID: cfg.InstanceID, HTTPAddr: cfg.AdvertiseHTTPAddr, GRPCAddr: cfg.AdvertiseGRPCAddr}, cfg.RouteLeaseTTL, cfg.RouteReconcileEvery)
	}
	gameSvrClient := farmrpc.NewClient(gameHTTPAddr)
	if mode != rpcgrpc.ModeHTTP {
		gameConn, err = rpcgrpc.Dial(gameGRPCTarget, clientCfg)
		if err != nil {
			panic(err)
		}
		gameSvrClient.WithGRPC(rpcv1.NewFarmServiceClient(gameConn), mode)
	}

	// 路线 3：Actor Runtime（热快照 + preValidate）。
	runtime := actor.NewRuntimeWithConfig(actor.Config{
		SchedulerShards: cfg.ActorShards, IngressCap: cfg.ActorIngress,
		FarmQueueCap: cfg.ActorFarmQueue, ReadyCap: cfg.ActorReadyQueue,
		Workers: cfg.ActorWorkers, MaxActiveActors: cfg.ActorMaxActive,
		EnqueueWait: cfg.ActorEnqueueWait, RetryAfter: cfg.AdmissionRetryAfter,
		ExecutionTimeout: cfg.ActorExecutionTimeout, IdleTTL: cfg.ActorIdleTTL,
	}, gameSvrClient, gameSvrClient, clock.System{}).WithObserver(metrics).WithRouteFencer(gameSvrClient)

	// 路线 4：farmsvc HTTP server，供 gatesvr 提交命令。
	svcMux := http.NewServeMux()
	var submitter realtime.CommandSubmitter = runtime
	var rdb = redisstore.New(cfg.RedisAddrs, cfg.RedisPassword)
	if cfg.RedisAddrs != "" {
		metrics.RegisterRedis("primary", rdb)
		submitter = realtime.NewPublishingSubmitter(runtime, realtime.NewPublisher(rdb), log).WithObserver(metrics)
	}
	farmCommandServer := farmsvc.NewServer(submitter)
	farmCommandServer.RegisterRoutes(svcMux)
	svcHTTP := &http.Server{Addr: cfg.HTTPAddr, Handler: metrics.Middleware(cfg.ServiceName, svcMux)}
	grpcOptions := rpcgrpc.ServerOptions(cfg.RPCMaxMessageBytes, cfg.RPCMaxStreams)
	grpcOptions = append(grpcOptions, grpc.ChainUnaryInterceptor(rpcgrpc.ServerTraceInterceptor(), metrics.GRPCServerInterceptor(cfg.ServiceName)))
	grpcServer := grpc.NewServer(grpcOptions...)
	grpcHealth := rpcgrpc.RegisterHealth(grpcServer)
	farmCommandServer.RegisterGRPC(grpcServer)

	application := app.New(cfg.ServiceName, cfg.DiagAddr, log, cfg.ShutdownTimeout)
	application.SetMetricsHandler(metrics.Handler())
	if cfg.RedisAddrs != "" {
		application.AddReadinessCheck("redis", probe.Redis(cfg.RedisAddrs))
	}
	if cfg.EtcdEndpoints != "" {
		application.AddReadinessCheck("etcd", probe.Etcd(cfg.EtcdEndpoints))
		application.AddReadinessCheck("gamesvr_discovery", gameResolver.Ready)
		application.AddReadinessCheck("route_controller", routeController.Ready)
	}
	application.AddShutdownHook(func(ctx context.Context) error {
		controlCancel()
		grpcHealth.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-ctx.Done():
			grpcServer.Stop()
		}
		if gameConn != nil {
			_ = gameConn.Close()
		}
		if cfg.RedisAddrs != "" {
			_ = rdb.Close()
		}
		if etcdClient != nil {
			_ = etcdClient.Close()
		}
		return svcHTTP.Shutdown(ctx)
	})
	application.MarkReady()

	if err := application.Run(func(ctx context.Context) error {
		runtime.Start(ctx)
		listener, err := net.Listen("tcp", cfg.NativeGRPCAddr)
		if err != nil {
			return err
		}
		errCh := make(chan error, 3)
		if routeController != nil {
			go func() { errCh <- routeController.Run(ctx) }()
		}
		go func() {
			log.Info("farmsvr native grpc serving", slog.String("addr", cfg.NativeGRPCAddr))
			errCh <- grpcServer.Serve(listener)
		}()
		go func() {
			log.Info("farmsvr legacy http rpc serving", slog.String("addr", cfg.HTTPAddr), slog.String("transport", cfg.RPCTransport))
			errCh <- svcHTTP.ListenAndServe()
		}()
		select {
		case <-ctx.Done():
			runtime.Wait()
			return nil
		case serveErr := <-errCh:
			if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, grpc.ErrServerStopped) {
				return nil
			}
			return serveErr
		}
	}); err != nil {
		log.Error("farmsvr exited with error", slog.String("error", err.Error()))
	}
}
