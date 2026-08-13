// gamesvr：唯一 MySQL 权威持久化提交层。
// 接收 farmsvr 的 CommitFarmCommand，在单事务内提交农场快照、资产、流水与 outbox。
// 同时为 gatesvr 提供 AccountService RPC（GuestLogin/Refresh/Authenticate）。
// 本 main 只做依赖装配与启动。
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"

	accountinfra "github.com/photon/farm-server/server/internal/account/infrastructure"
	"github.com/photon/farm-server/server/internal/account/transport/accountrpc"
	catalogdomain "github.com/photon/farm-server/server/internal/catalog/domain"
	cataloginfra "github.com/photon/farm-server/server/internal/catalog/infrastructure"
	"github.com/photon/farm-server/server/internal/catalog/transport/catalogrpc"
	economydomain "github.com/photon/farm-server/server/internal/economy/domain"
	economyinfra "github.com/photon/farm-server/server/internal/economy/infrastructure"
	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	farmapp "github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	mailinfra "github.com/photon/farm-server/server/internal/mail/infrastructure"
	mailrealtime "github.com/photon/farm-server/server/internal/mail/realtime"
	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	petinfra "github.com/photon/farm-server/server/internal/pet/infrastructure"
	"github.com/photon/farm-server/server/internal/pet/transport/petrpc"
	socialinfra "github.com/photon/farm-server/server/internal/social/infrastructure"
	"github.com/photon/farm-server/server/internal/social/transport/socialrpc"
	taskdomain "github.com/photon/farm-server/server/internal/task/domain"
	taskinfra "github.com/photon/farm-server/server/internal/task/infrastructure"
	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
	"github.com/photon/farm-server/server/pkg/admission"
	"github.com/photon/farm-server/server/pkg/app"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/config"
	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/logging"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/probe"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"github.com/photon/farm-server/server/pkg/shard"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type accountAggregate interface {
	accountrpc.AccountSvc
	farmrpc.OwnerDisplayNameLoader
}

type farmAggregate interface {
	farmapp.Committer
	farmapp.SnapshotLoader
	farmapp.RouteFencer
}

func main() {
	cfg, err := config.Load(true) // gamesvr 需要 MySQL
	if err != nil {
		panic(err)
	}
	log := logging.New(cfg.ServiceName, cfg.InstanceID, cfg.AppEnv, cfg.LogLevel)
	metrics := observability.New(cfg.ServiceName, log)
	http.DefaultTransport = metrics.Transport(cfg.ServiceName, http.DefaultTransport)

	pools := make(map[string]*sql.DB, len(cfg.MySQLShards))
	// 连接池参数（见路线 6.5 性能排查 P0）。
	// MaxOpenConns=25 防止突发请求耗尽 MySQL max_connections；
	// ConnMaxLifetime=5m 周期回收，避免 MySQL 8h 默认超时断连。
	maxOpen := cfg.MySQLTotalConnections / (cfg.GameExpectedInstances * len(cfg.MySQLShards))
	if maxOpen < 2 {
		maxOpen = 2
	}
	maxIdle := maxOpen / 2
	if maxIdle < 1 {
		maxIdle = 1
	}
	for _, shardCfg := range cfg.MySQLShards {
		db, openErr := sql.Open("mysql", shardCfg.DSN)
		if openErr != nil {
			log.Error("failed to open mysql shard", slog.String("shard", shardCfg.Name), slog.String("error", openErr.Error()))
			panic(openErr)
		}
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		db.SetConnMaxLifetime(5 * time.Minute)
		db.SetConnMaxIdleTime(30 * time.Second)
		metrics.RegisterDB(shardCfg.Name, db)
		pools[shardCfg.Name] = db
		defer db.Close()
	}
	primaryDB := pools[cfg.MySQLShards[0].Name]
	var storage farmAggregate
	var router *shard.Router

	// 路线 4：账号服务（GuestLogin/Refresh/Authenticate/InitAccount）。
	// refresh_token 存 Redis（路线 8.5 P2），TTL 自动过期，替代 MySQL sessions 表。
	rdb := redisstore.New(cfg.RedisAddrs, cfg.RedisPassword)
	metrics.RegisterRedis("primary", rdb)
	refreshStore := redisstore.NewRefreshStore(rdb)
	var accountSvc accountAggregate
	if len(cfg.MySQLShards) == 1 {
		storage = infrastructure.NewMySQLCommitter(primaryDB, clock.System{}).WithObserver(metrics)
		accountSvc = accountinfra.NewMySQLAccountService(primaryDB, clock.System{}, []byte(cfg.TokenSecret), refreshStore)
	} else {
		names := make([]string, 0, len(cfg.MySQLShards))
		for _, shardCfg := range cfg.MySQLShards {
			names = append(names, shardCfg.Name)
		}
		var routeErr error
		router, routeErr = shard.NewRouter(names)
		if routeErr != nil {
			panic(routeErr)
		}
		backends := make(map[string]infrastructure.ShardCommitBackend, len(names))
		accounts := make(map[string]*accountinfra.MySQLAccountService, len(names))
		epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for index, name := range router.Shards() {
			committer := infrastructure.NewMySQLCommitter(pools[name], clock.System{}).
				WithObserver(metrics).
				WithFriendshipEdges().
				WithDeferredRemoteStealCredit(func(ownerUserID, actorUserID int64) bool {
					ownerShard, ownerErr := router.ShardForUserID(ownerUserID)
					actorShard, actorErr := router.ShardForUserID(actorUserID)
					return ownerErr == nil && actorErr == nil && ownerShard != actorShard
				})
			backends[name] = infrastructure.ShardCommitBackend{Committer: committer, Loader: committer, Fencer: committer}
			generator, genErr := shard.NewIDGenerator(epoch, uint8(index), uint8(cfg.GlobalIDNodeID))
			if genErr != nil {
				panic(genErr)
			}
			accounts[name] = accountinfra.NewMySQLAccountService(pools[name], clock.System{}, []byte(cfg.TokenSecret), refreshStore).WithIDGenerator(generator)
		}
		storage, err = infrastructure.NewShardedCommitter(router, backends)
		if err != nil {
			panic(err)
		}
		accountSvc, err = accountinfra.NewShardedAccountService(router, accounts)
		if err != nil {
			panic(err)
		}
	}

	// 路线 7 §10.2：好友服务（CreateInvite/AcceptInvite/AreFriends）。
	// InviteStore 使用 Redis 存储邀请码（TTL 30min，有效期内可复用）。
	inviteStore := redisstore.NewInviteStore(rdb)
	var socialSvc socialrpc.SocialSvc
	if len(cfg.MySQLShards) == 1 {
		socialSvc = socialinfra.NewMySQLSocialService(primaryDB, inviteStore)
	} else {
		socialServices := make(map[string]*socialinfra.MySQLSocialService, len(cfg.MySQLShards))
		socialSagas := make(map[string]*socialinfra.FriendEdgeSaga, len(cfg.MySQLShards))
		for _, shardCfg := range cfg.MySQLShards {
			socialServices[shardCfg.Name] = socialinfra.NewMySQLSocialService(pools[shardCfg.Name], inviteStore)
			socialSagas[shardCfg.Name] = socialinfra.NewFriendEdgeSaga(pools[shardCfg.Name])
		}
		socialSvc, err = socialinfra.NewShardedSocialService(router, socialServices, socialSagas, inviteStore)
		if err != nil {
			panic(err)
		}
	}

	// 路线 7 §10.4：邮件服务（SendMail/ListMails/ClaimAttachment）。
	mailNotifier := mailrealtime.NewPublisher(rdb).WithObserver(metrics)
	var mailSvc maildomain.MailService = mailinfra.NewMySQLMailService(primaryDB).WithNotifier(mailNotifier)

	// 路线 8：任务服务（IncrProgress/ListTasks/ClaimReward）。
	var taskSvc taskdomain.TaskService = taskinfra.NewMySQLTaskService(primaryDB)

	// 路线 8：宠物服务（BuyPet/HasPet）。
	var petSvc petrpc.PetSvc = petinfra.NewMySQLPetService(primaryDB)
	// 路线 9.8：图鉴与私有玩家资产只读服务。
	var catalogSvc catalogdomain.Service = cataloginfra.NewMySQLCatalogService(primaryDB)
	var assetSvc economydomain.AssetService = economyinfra.NewMySQLAssetService(primaryDB)
	if len(cfg.MySQLShards) > 1 {
		mailServices := make(map[string]*mailinfra.MySQLMailService, len(pools))
		taskServices := make(map[string]*taskinfra.MySQLTaskService, len(pools))
		petServices := make(map[string]*petinfra.MySQLPetService, len(pools))
		catalogServices := make(map[string]*cataloginfra.MySQLCatalogService, len(pools))
		assetServices := make(map[string]*economyinfra.MySQLAssetService, len(pools))
		for name, db := range pools {
			mailServices[name] = mailinfra.NewMySQLMailService(db).WithNotifier(mailNotifier)
			taskServices[name] = taskinfra.NewMySQLTaskService(db)
			petServices[name] = petinfra.NewMySQLPetService(db)
			catalogServices[name] = cataloginfra.NewMySQLCatalogService(db)
			assetServices[name] = economyinfra.NewMySQLAssetService(db)
		}
		mailSvc, err = mailinfra.NewShardedMailService(router, mailServices)
		if err != nil {
			panic(err)
		}
		taskSvc, err = taskinfra.NewShardedTaskService(router, taskServices)
		if err != nil {
			panic(err)
		}
		petSvc, err = petinfra.NewShardedPetService(router, petServices)
		if err != nil {
			panic(err)
		}
		catalogSvc, err = cataloginfra.NewShardedCatalogService(router, catalogServices)
		if err != nil {
			panic(err)
		}
		assetSvc, err = economyinfra.NewShardedAssetService(router, assetServices)
		if err != nil {
			panic(err)
		}
	}

	// 内部 RPC mux（GRPCAddr：:9090）。
	farmServer := farmrpc.NewServer(storage, storage, accountSvc)
	accountServer := accountrpc.NewServer(accountSvc)
	socialServer := socialrpc.NewServer(socialSvc)
	mailServer := mailrpc.NewServer(mailSvc)
	taskServer := taskrpc.NewServer(taskSvc)
	petServer := petrpc.NewServer(petSvc)
	catalogServer := catalogrpc.NewServer(catalogSvc)
	assetServer := assetrpc.NewServer(assetSvc)
	rpcMux := http.NewServeMux()
	farmServer.RegisterRoutes(rpcMux)
	accountServer.RegisterRoutes(rpcMux)
	socialServer.RegisterRoutes(rpcMux)
	mailServer.RegisterRoutes(rpcMux)
	taskServer.RegisterRoutes(rpcMux)
	petServer.RegisterRoutes(rpcMux)
	catalogServer.RegisterRoutes(rpcMux)
	assetServer.RegisterRoutes(rpcMux)
	gate := admission.NewObservedGate(cfg.GameMaxInflight, admission.ReasonGamesvrGlobal, metrics)
	rpcHandler := admission.InflightWithGate(gate, cfg.AdmissionWait, cfg.AdmissionRetryAfter, rpcMux)
	rpcHTTP := &http.Server{Addr: cfg.GRPCAddr, Handler: metrics.Middleware(cfg.ServiceName, rpcHandler)}
	grpcOptions := rpcgrpc.ServerOptions(cfg.RPCMaxMessageBytes, cfg.RPCMaxStreams)
	grpcOptions = append(grpcOptions, grpc.ChainUnaryInterceptor(rpcgrpc.ServerTraceInterceptor(), metrics.GRPCServerInterceptor(cfg.ServiceName), rpcgrpc.AdmissionInterceptor(gate, cfg.AdmissionWait, cfg.AdmissionRetryAfter)))
	grpcServer := grpc.NewServer(grpcOptions...)
	grpcHealth := rpcgrpc.RegisterHealth(grpcServer)
	farmServer.RegisterGRPC(grpcServer)
	accountServer.RegisterGRPC(grpcServer)
	socialServer.RegisterGRPC(grpcServer)
	mailServer.RegisterGRPC(grpcServer)
	taskServer.RegisterGRPC(grpcServer)
	petServer.RegisterGRPC(grpcServer)
	catalogServer.RegisterGRPC(grpcServer)
	assetServer.RegisterGRPC(grpcServer)
	var etcdClient *clientv3.Client
	if cfg.EtcdEndpoints != "" {
		etcdClient, err = discovery.NewEtcdClient(cfg.EtcdEndpoints, cfg.EtcdUsername, cfg.EtcdPassword)
		if err != nil {
			panic(err)
		}
	}

	application := app.New(cfg.ServiceName, cfg.DiagAddr, log, cfg.ShutdownTimeout)
	application.SetMetricsHandler(metrics.Handler())
	for _, shardCfg := range cfg.MySQLShards {
		application.AddReadinessCheck("mysql_"+shardCfg.Name, probe.MySQL(pools[shardCfg.Name]))
	}
	if cfg.EtcdEndpoints != "" {
		application.AddReadinessCheck("etcd", probe.Etcd(cfg.EtcdEndpoints))
	}
	application.AddShutdownHook(func(ctx context.Context) error {
		grpcHealth.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		_ = rdb.Close()
		if etcdClient != nil {
			_ = etcdClient.Close()
		}
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-ctx.Done():
			grpcServer.Stop()
		}
		return rpcHTTP.Shutdown(ctx)
	})
	application.MarkReady()

	if err := application.Run(func(ctx context.Context) error {
		listener, err := net.Listen("tcp", cfg.NativeGRPCAddr)
		if err != nil {
			return err
		}
		errCh := make(chan error, 3)
		if etcdClient != nil {
			endpoint := discovery.Endpoint{InstanceID: cfg.InstanceID, HTTPAddr: cfg.AdvertiseHTTPAddr, GRPCAddr: cfg.AdvertiseGRPCAddr}
			go func() {
				errCh <- discovery.Register(ctx, etcdClient, cfg.EtcdPrefix, "gamesvr", endpoint, cfg.RouteLeaseTTL)
			}()
		}
		go func() {
			log.Info("gamesvr native grpc serving", slog.String("addr", cfg.NativeGRPCAddr))
			errCh <- grpcServer.Serve(listener)
		}()
		go func() {
			log.Info("gamesvr legacy http rpc serving", slog.String("addr", cfg.GRPCAddr))
			errCh <- rpcHTTP.ListenAndServe()
		}()
		select {
		case <-ctx.Done():
			return nil
		case serveErr := <-errCh:
			if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, grpc.ErrServerStopped) {
				return nil
			}
			return serveErr
		}
	}); err != nil {
		log.Error("gamesvr exited with error", slog.String("error", err.Error()))
	}
}
