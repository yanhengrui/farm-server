// gatesvr：HTTP/WSS 接入、认证、Session 串行、client_seq/server_seq+ACK、路由转发。
// 不持有 Farm Actor，不写权威业务事实（见 CONSTITUTION §1.1）。
// 本 main 只做依赖装配与启动。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/account/transport/accountrpc"
	"github.com/photon/farm-server/server/internal/catalog/transport/catalogrpc"
	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	farmdomain "github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/realtime"
	"github.com/photon/farm-server/server/internal/farm/routing"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	ghttp "github.com/photon/farm-server/server/internal/gateway/http"
	gws "github.com/photon/farm-server/server/internal/gateway/ws"
	mailrealtime "github.com/photon/farm-server/server/internal/mail/realtime"
	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	"github.com/photon/farm-server/server/internal/pet/transport/petrpc"
	"github.com/photon/farm-server/server/internal/social/transport/socialrpc"
	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
	"github.com/photon/farm-server/server/pkg/admission"
	"github.com/photon/farm-server/server/pkg/app"
	"github.com/photon/farm-server/server/pkg/config"
	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/logging"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/probe"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"github.com/photon/farm-server/server/pkg/session"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load(false) // gatesvr 不直连 MySQL
	if err != nil {
		panic(err)
	}
	log := logging.New(cfg.ServiceName, cfg.InstanceID, cfg.AppEnv, cfg.LogLevel)
	metrics := observability.New(cfg.ServiceName, log)
	http.DefaultTransport = metrics.Transport(cfg.ServiceName, http.DefaultTransport)
	secret := []byte(cfg.TokenSecret)
	wsCtx, wsCancel := context.WithCancel(context.Background())
	mode := rpcgrpc.ParseMode(cfg.RPCTransport)
	clientCfg := rpcgrpc.ClientConfig{DefaultTimeout: cfg.RPCDefaultTimeout, KeepaliveTime: cfg.RPCKeepaliveTime, KeepaliveTimeout: cfg.RPCKeepaliveTimeout, MaxMessageBytes: cfg.RPCMaxMessageBytes, UnaryInterceptors: []grpc.UnaryClientInterceptor{metrics.GRPCClientInterceptor(cfg.ServiceName)}}
	var gameConn, farmConn *grpc.ClientConn
	var etcdClient *clientv3.Client
	var routedFarmClient *routing.Client
	var farmRouteResolver *routing.Resolver
	var gameResolver *discovery.Resolver
	var farmClient gws.FarmCommandClient
	gameHTTPAddr, gameGRPCTarget := cfg.GameSvrAddr, cfg.GameSvrGRPCAddr
	if cfg.EtcdEndpoints != "" {
		etcdClient, err = discovery.NewEtcdClient(cfg.EtcdEndpoints, cfg.EtcdUsername, cfg.EtcdPassword)
		if err != nil {
			panic(err)
		}
		gameResolver = discovery.NewResolver(etcdClient, cfg.EtcdPrefix, "gamesvr")
		if err = gameResolver.Start(wsCtx); err != nil {
			panic(err)
		}
		http.DefaultTransport = &discovery.HTTPTransport{Base: http.DefaultTransport, LogicalHost: "gamesvr.internal", Resolver: gameResolver}
		gameHTTPAddr = "http://gamesvr.internal"
		if mode != rpcgrpc.ModeHTTP {
			builder := &discovery.GRPCBuilder{SchemeName: "farm-gamesvr", Resolver: gameResolver}
			gameCfg := clientCfg
			gameCfg.Resolver = builder
			gameCfg.LoadBalancing = "round_robin"
			gameGRPCTarget = builder.Scheme() + ":///gamesvr"
			gameConn, err = rpcgrpc.Dial(gameGRPCTarget, gameCfg)
			if err != nil {
				panic(err)
			}
		}
		farmRouteResolver = routing.NewResolver(etcdClient, cfg.EtcdPrefix, cfg.RouteBucketCount)
		if err = farmRouteResolver.Start(wsCtx); err != nil {
			panic(err)
		}
		routedFarmClient = routing.NewClient(farmRouteResolver, mode, clientCfg)
		farmClient = routedFarmClient
	} else {
		staticFarmClient := farmsvc.NewClient(cfg.FarmSvrAddr)
		farmClient = staticFarmClient
		if mode != rpcgrpc.ModeHTTP {
			gameConn, err = rpcgrpc.Dial(gameGRPCTarget, clientCfg)
			if err != nil {
				panic(err)
			}
			farmConn, err = rpcgrpc.Dial(cfg.FarmSvrGRPCAddr, clientCfg)
			if err != nil {
				panic(err)
			}
			staticFarmClient.WithGRPC(rpcv1.NewFarmCommandServiceClient(farmConn), mode)
		}
	}
	if mode != rpcgrpc.ModeHTTP && gameConn == nil {
		gameConn, err = rpcgrpc.Dial(gameGRPCTarget, clientCfg)
		if err != nil {
			panic(err)
		}
	}

	// 路线 4：accountrpc 客户端（gatesvr → gamesvr 账号服务）。
	acctClient := accountrpc.NewClient(gameHTTPAddr)

	// 路线 3/4：farmrpc 客户端（gatesvr → gamesvr 获取快照）。
	farmRPCClient := farmrpc.NewClient(gameHTTPAddr)
	socialClient := socialrpc.NewClient(gameHTTPAddr)
	mailClient := mailrpc.NewClient(gameHTTPAddr)
	taskClient := taskrpc.NewClient(gameHTTPAddr)
	petClient := petrpc.NewClient(gameHTTPAddr)
	catalogClient := catalogrpc.NewClient(gameHTTPAddr)
	assetClient := assetrpc.NewClient(gameHTTPAddr)
	if gameConn != nil {
		acctClient.WithGRPC(rpcv1.NewAccountServiceClient(gameConn), mode)
		farmRPCClient.WithGRPC(rpcv1.NewFarmServiceClient(gameConn), mode)
		socialClient.WithGRPC(rpcv1.NewSocialServiceClient(gameConn), mode)
		mailClient.WithGRPC(rpcv1.NewMailServiceClient(gameConn), mode)
		taskClient.WithGRPC(rpcv1.NewTaskServiceClient(gameConn), mode)
		petClient.WithGRPC(rpcv1.NewPetServiceClient(gameConn), mode)
		catalogClient.WithGRPC(rpcv1.NewCatalogServiceClient(gameConn), mode)
		assetClient.WithGRPC(rpcv1.NewAssetServiceClient(gameConn), mode)
	}
	rdb := redisstore.New(cfg.RedisAddrs, cfg.RedisPassword)
	var readCache *ghttp.ReadCache
	var snapshotReadClient ghttp.FarmSnapshotClient = ghttp.NewSingleflightSnapshotClient(farmRPCClient)
	var assetReadClient ghttp.PlayerAssetClient = ghttp.NewSingleflightAssetClient(assetClient)
	if cfg.RedisAddrs != "" && cfg.GatewayReadCacheTTL > 0 {
		readCache = ghttp.NewReadCache(rdb, "farm:"+cfg.AppEnv+":read", cfg.GatewayReadCacheTTL, cfg.GatewayReadCacheStaleTTL, cfg.GatewayReadCacheTimeout, metrics)
		snapshotReadClient = ghttp.NewCachedSnapshotClient(snapshotReadClient, readCache)
		assetReadClient = ghttp.NewCachedAssetClient(assetReadClient, readCache)
	}

	mux := http.NewServeMux()
	ghttp.NewAuthHandler(acctClient).RegisterRoutes(mux)
	ghttp.NewFarmHandler(secret, snapshotReadClient).RegisterRoutes(mux)
	ghttp.NewShopHandler(secret, farmRPCClient).WithReadCache(readCache).RegisterRoutes(mux)
	ghttp.NewSocialHandler(secret, socialClient).RegisterRoutes(mux)
	ghttp.NewMailHandler(secret, mailClient).WithReadCache(readCache).RegisterRoutes(mux)
	ghttp.NewTaskHandler(secret, taskClient).WithReadCache(readCache).RegisterRoutes(mux)
	ghttp.NewPetHandler(secret, petClient).WithReadCache(readCache).RegisterRoutes(mux)
	ghttp.NewCatalogHandler(secret, catalogClient).RegisterRoutes(mux)
	ghttp.NewPlayerHandler(secret, assetReadClient).RegisterRoutes(mux)
	var authSessions *redisstore.RefreshStore
	wsHandler := gws.NewHandler(wsCtx, secret, farmClient, log)
	if readCache != nil {
		wsHandler.WithCommittedHook(func(ctx context.Context, command farmdomain.Command) {
			readCache.Invalidate(ctx, command.FarmID, command.ActorUser)
		})
	}
	if cfg.RedisAddrs != "" {
		metrics.RegisterRedis("primary", rdb)
		authSessions = redisstore.NewRefreshStore(rdb)
		connStore := redisstore.NewConnStore(rdb, cfg.InstanceID)
		if err := connStore.Start(wsCtx); err != nil {
			panic(err)
		}
		wsHandler.WithRedis(gws.HandlerDeps{SessionStore: redisstore.NewSessionStore(rdb), AuthSessions: authSessions, ConnStore: connStore, InstanceID: cfg.InstanceID})
		mailboxSubscriber := mailrealtime.NewSubscriber(rdb, cfg.InstanceID)
		if err := mailboxSubscriber.Start(wsCtx, func(message mailrealtime.Message) {
			wsHandler.PushMailboxChanged(message.UserID, message.UnreadCount, message.Version)
		}); err != nil {
			panic(err)
		}
		if err := wsHandler.WithFarmEvents(realtime.NewSubscriber(rdb, cfg.PubSubUnsubscribeWait).WithObserver(metrics)); err != nil {
			panic(err)
		}
	}
	userLimiter := admission.NewLimiter(float64(cfg.GatewayUserRPS), cfg.GatewayUserBurst, 10*time.Minute)
	ipLimiter := admission.NewLimiter(float64(cfg.GatewayIPRPS), cfg.GatewayIPBurst, 10*time.Minute)
	wsHandler.WithMetrics(metrics).WithCommandLimiter(userLimiter, cfg.AdmissionRetryAfter)
	wsHandler.RegisterRoutes(mux)
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"pong":true}`))
	})
	var routedHandler http.Handler = mux
	if authSessions != nil {
		routedHandler = ghttp.RequireActiveSession(secret, authSessions, routedHandler)
	}
	rateLimited := admission.RateLimitBy(ipLimiter, userLimiter, cfg.AdmissionRetryAfter, func(r *http.Request) string {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			return ""
		}
		userID, err := session.Parse(token, secret)
		if err != nil {
			return ""
		}
		return admission.UserKey(userID)
	}, routedHandler)
	publicHandler := http.Handler(rateLimited)
	if cfg.GatewayMaxInflight > 0 {
		gate := admission.NewObservedGate(cfg.GatewayMaxInflight, admission.ReasonGatewayGlobal, metrics)
		publicHandler = admission.PublicInflightWithGate(gate, cfg.AdmissionWait, cfg.AdmissionRetryAfter, func(r *http.Request) bool {
			return r.URL.Path == "/ws"
		}, publicHandler)
	}
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: metrics.Middleware(cfg.ServiceName, publicHandler)}

	application := app.New(cfg.ServiceName, cfg.DiagAddr, log, cfg.ShutdownTimeout)
	application.SetMetricsHandler(metrics.Handler())
	if cfg.RedisAddrs != "" {
		application.AddReadinessCheck("redis", probe.Redis(cfg.RedisAddrs))
	}
	if cfg.EtcdEndpoints != "" {
		application.AddReadinessCheck("etcd", probe.Etcd(cfg.EtcdEndpoints))
		application.AddReadinessCheck("gamesvr_discovery", gameResolver.Ready)
		application.AddReadinessCheck("farm_routes", farmRouteResolver.Ready)
	}
	application.AddShutdownHook(func(ctx context.Context) error {
		wsHandler.BeginHandoff(ctx)
		wsCancel()
		if cfg.RedisAddrs != "" {
			_ = rdb.Close()
		}
		if gameConn != nil {
			_ = gameConn.Close()
		}
		if farmConn != nil {
			_ = farmConn.Close()
		}
		if routedFarmClient != nil {
			_ = routedFarmClient.Close()
		}
		if etcdClient != nil {
			_ = etcdClient.Close()
		}
		return httpServer.Shutdown(ctx)
	})
	application.MarkReady()

	if err := application.Run(func(ctx context.Context) error {
		log.Info("gatesvr serving", slog.String("http", cfg.HTTPAddr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}); err != nil {
		log.Error("gatesvr exited with error", slog.String("error", err.Error()))
	}
}
