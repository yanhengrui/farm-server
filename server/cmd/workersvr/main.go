// workersvr: Outbox Relay + Kafka Consumer + 宠物调度。
// 职责：扫描 outbox_events PENDING 行发布到消息总线；幂等消费并投影；定时扫描宠物收获。
// 不直接裁决农场状态或资产结果（见 CONSTITUTION §1.1）。
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/internal/farm/routing"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	socialinfra "github.com/photon/farm-server/server/internal/social/infrastructure"
	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
	"github.com/photon/farm-server/server/internal/worker"
	"github.com/photon/farm-server/server/pkg/app"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/config"
	"github.com/photon/farm-server/server/pkg/discovery"
	"github.com/photon/farm-server/server/pkg/logging"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/probe"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"github.com/photon/farm-server/server/pkg/shard"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.LoadForService()
	if err != nil {
		panic(err)
	}
	log := logging.New(cfg.ServiceName, cfg.InstanceID, cfg.AppEnv, cfg.LogLevel)
	metrics := observability.New(cfg.ServiceName, log)
	http.DefaultTransport = metrics.Transport(cfg.ServiceName, http.DefaultTransport)
	pools := make(map[string]*sql.DB, len(cfg.MySQLShards))
	for _, shardCfg := range cfg.MySQLShards {
		db, openErr := sql.Open("mysql", shardCfg.DSN)
		if openErr != nil {
			panic(openErr)
		}
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(2)
		db.SetConnMaxLifetime(5 * time.Minute)
		db.SetConnMaxIdleTime(30 * time.Second)
		metrics.RegisterDB(shardCfg.Name, db)
		metrics.RegisterOutboxShard(shardCfg.Name, db)
		metrics.RegisterPetScannerShard(shardCfg.Name, db)
		pools[shardCfg.Name] = db
		defer db.Close()
	}
	primaryDB := pools[cfg.MySQLShards[0].Name]
	shardNames := make([]string, 0, len(cfg.MySQLShards))
	for _, shardCfg := range cfg.MySQLShards {
		shardNames = append(shardNames, shardCfg.Name)
	}
	shardRouter, err := shard.NewRouter(shardNames)
	if err != nil {
		panic(err)
	}

	application := app.New(cfg.ServiceName, cfg.DiagAddr, log, cfg.ShutdownTimeout)
	application.SetMetricsHandler(metrics.Handler())
	for _, shardCfg := range cfg.MySQLShards {
		application.AddReadinessCheck("mysql_"+shardCfg.Name, probe.MySQL(pools[shardCfg.Name]))
	}
	if cfg.EtcdEndpoints != "" {
		application.AddReadinessCheck("etcd", probe.Etcd(cfg.EtcdEndpoints))
	}
	if cfg.EventBusDriver == "kafka" {
		application.AddReadinessCheck("kafka", probe.Kafka(cfg.KafkaBrokers))
	}
	application.MarkReady()

	if err := application.Run(func(ctx context.Context) error {
		mode := rpcgrpc.ParseMode(cfg.RPCTransport)
		var gameConn, farmConn *grpc.ClientConn
		var etcdClient *clientv3.Client
		var routedFarmClient *routing.Client
		var farmClient worker.FarmCommandClient
		gameHTTPAddr, gameGRPCTarget := cfg.GameSvrAddr, cfg.GameSvrGRPCAddr
		clientCfg := rpcgrpc.ClientConfig{DefaultTimeout: cfg.RPCDefaultTimeout, KeepaliveTime: cfg.RPCKeepaliveTime, KeepaliveTimeout: cfg.RPCKeepaliveTimeout, MaxMessageBytes: cfg.RPCMaxMessageBytes, UnaryInterceptors: []grpc.UnaryClientInterceptor{metrics.GRPCClientInterceptor(cfg.ServiceName)}}
		if cfg.EtcdEndpoints != "" {
			etcdClient, err = discovery.NewEtcdClient(cfg.EtcdEndpoints, cfg.EtcdUsername, cfg.EtcdPassword)
			if err != nil {
				return err
			}
			defer etcdClient.Close()
			gameResolver := discovery.NewResolver(etcdClient, cfg.EtcdPrefix, "gamesvr")
			if err = gameResolver.Start(ctx); err != nil {
				return err
			}
			http.DefaultTransport = &discovery.HTTPTransport{Base: http.DefaultTransport, LogicalHost: "gamesvr.internal", Resolver: gameResolver}
			gameHTTPAddr = "http://gamesvr.internal"
			if mode != rpcgrpc.ModeHTTP {
				builder := &discovery.GRPCBuilder{SchemeName: "farm-gamesvr", Resolver: gameResolver}
				clientCfg.Resolver, clientCfg.LoadBalancing = builder, "round_robin"
				gameGRPCTarget = builder.Scheme() + ":///gamesvr"
			}
			farmResolver := routing.NewResolver(etcdClient, cfg.EtcdPrefix, cfg.RouteBucketCount)
			if err = farmResolver.Start(ctx); err != nil {
				return err
			}
			routedFarmClient = routing.NewClient(farmResolver, mode, rpcgrpc.ClientConfig{DefaultTimeout: cfg.RPCDefaultTimeout, KeepaliveTime: cfg.RPCKeepaliveTime, KeepaliveTimeout: cfg.RPCKeepaliveTimeout, MaxMessageBytes: cfg.RPCMaxMessageBytes, UnaryInterceptors: []grpc.UnaryClientInterceptor{metrics.GRPCClientInterceptor(cfg.ServiceName)}})
			defer routedFarmClient.Close()
			farmClient = routedFarmClient
		} else {
			staticFarmClient := farmsvc.NewClient(cfg.FarmSvrAddr)
			farmClient = staticFarmClient
		}
		if mode != rpcgrpc.ModeHTTP {
			gameConn, err = rpcgrpc.Dial(gameGRPCTarget, clientCfg)
			if err != nil {
				return err
			}
			defer gameConn.Close()
			if cfg.EtcdEndpoints == "" {
				farmConn, err = rpcgrpc.Dial(cfg.FarmSvrGRPCAddr, clientCfg)
				if err != nil {
					return err
				}
				farmClient.(*farmsvc.Client).WithGRPC(rpcv1.NewFarmCommandServiceClient(farmConn), mode)
				defer farmConn.Close()
			}
		}

		pub := buildPublisher(cfg, log)
		defer pub.Close()

		primaryRelay := infrastructure.NewOutboxRelay(primaryDB, pub, clock.System{}, log, infrastructure.OutboxRelayConfig{
			BatchSize: cfg.OutboxBatchSize,
			MaxRetry:  cfg.OutboxMaxRetry,
			LockTTL:   cfg.OutboxLockTTL,
			Topic:     cfg.KafkaTopic,
		}).WithObserver(metrics)
		var relay outboxScanner = primaryRelay
		if len(cfg.MySQLShards) > 1 {
			shardRelays := make(map[string]infrastructure.OutboxScanner, len(cfg.MySQLShards))
			for _, shardCfg := range cfg.MySQLShards {
				shardRelays[shardCfg.Name] = infrastructure.NewOutboxRelay(pools[shardCfg.Name], pub, clock.System{}, log, infrastructure.OutboxRelayConfig{
					BatchSize: cfg.OutboxBatchSize,
					MaxRetry:  cfg.OutboxMaxRetry,
					LockTTL:   cfg.OutboxLockTTL,
					Topic:     cfg.KafkaTopic,
				}).WithObserver(metrics)
			}
			multiRelay, relayErr := infrastructure.NewShardedOutboxScanner(shardRelays)
			if relayErr != nil {
				return relayErr
			}
			relay = multiRelay
		}

		dedup := infrastructure.NewConsumerDedup(primaryDB)
		shardDedups := make(map[string]*infrastructure.ConsumerDedup, len(pools))
		for name, db := range pools {
			shardDedups[name] = infrastructure.NewConsumerDedup(db)
		}
		dedupResolver, err := infrastructure.NewShardedConsumerDedupResolver(shardRouter, shardDedups, dedup)
		if err != nil {
			return err
		}
		projector := infrastructure.NewLogProjector(log)

		// 路线 7 §10.4：MailProjector 消费 farm.stolen.v1 和 social.friend_accepted.v1。
		mailClient := mailrpc.NewClient(gameHTTPAddr)
		taskClient := taskrpc.NewClient(gameHTTPAddr)
		if gameConn != nil {
			mailClient.WithGRPC(rpcv1.NewMailServiceClient(gameConn), mode)
			taskClient.WithGRPC(rpcv1.NewTaskServiceClient(gameConn), mode)
		}
		mailProjector := infrastructure.NewMailProjector(mailClient, log)

		// 路线 8：TaskProjector 消费 farm 事件推进任务进度。
		taskProjector := infrastructure.NewTaskProjector(taskClient, log)

		// 路线 8.5：CatalogProjector 消费 farm.harvested.v1 解锁图鉴。
		var catalogProjector infrastructure.EventProjector = infrastructure.NewCatalogProjector(primaryDB, log)

		// 路线 8：PetScanner 宠物自动收割（通过 farmsvc 提交 CmdPetAutoHarvest）。
		petOptions := func(shardName string) worker.PetScannerOptions {
			return worker.PetScannerOptions{
				Shard:      shardName,
				LeaseTTL:   cfg.PetScanLeaseTTL,
				Workers:    cfg.PetScanWorkers,
				RatePerSec: cfg.PetScanRatePerSecond,
				Metrics:    metrics,
			}
		}
		var petScanner petScanner = worker.NewPetScannerWithOptions(primaryDB, farmClient, log, petOptions(cfg.MySQLShards[0].Name))
		if len(cfg.MySQLShards) > 1 {
			shardedCatalogProjector, projectorErr := infrastructure.NewShardedCatalogProjector(shardRouter, pools, log)
			if projectorErr != nil {
				return projectorErr
			}
			catalogProjector = shardedCatalogProjector
			petScanners := make(map[string]*worker.PetScanner, len(pools))
			for name, db := range pools {
				petScanners[name] = worker.NewPetScannerWithOptions(db, farmClient, log, petOptions(name))
			}
			shardedPetScanner, scannerErr := worker.NewShardedPetScanner(petScanners)
			if scannerErr != nil {
				return scannerErr
			}
			petScanner = shardedPetScanner
		}

		// 路线 10.2：保留期清理。只清理已终结的异步链路记录；
		// DEAD outbox 与 economy_transactions 不在范围内（见 retention_reaper.go 包注释）。
		retentionPolicy := infrastructure.RetentionPolicy{
			PublishedOutbox: cfg.RetentionOutbox,
			CmdReceipts:     cfg.RetentionCmdReceipts,
			ConsumedEvents:  cfg.RetentionConsumedEvents,
		}
		var reaper retentionReaper = infrastructure.NewRetentionReaper(primaryDB, clock.System{}, log, infrastructure.RetentionReaperConfig{
			Policy:    retentionPolicy,
			BatchSize: cfg.RetentionBatchSize,
			MaxPerRun: cfg.RetentionMaxPerRun,
		}).WithObserver(metrics)
		if len(cfg.MySQLShards) > 1 {
			reapers := make(map[string]*infrastructure.RetentionReaper, len(pools))
			for name, db := range pools {
				reapers[name] = infrastructure.NewRetentionReaper(db, clock.System{}, log, infrastructure.RetentionReaperConfig{
					Policy: retentionPolicy, BatchSize: cfg.RetentionBatchSize, MaxPerRun: cfg.RetentionMaxPerRun,
				}).WithObserver(metrics)
			}
			shardedReaper, reaperErr := infrastructure.NewShardedRetentionReaper(reapers)
			if reaperErr != nil {
				return reaperErr
			}
			reaper = shardedReaper
		}
		if retentionPolicy.Enabled() && cfg.RetentionInterval > 0 {
			go runRetentionLoop(ctx, reaper, log, cfg.RetentionInterval)
			log.Info("retention reaper started",
				slog.Duration("interval", cfg.RetentionInterval),
				slog.Duration("outbox_published", cfg.RetentionOutbox),
				slog.Duration("cmd_receipts", cfg.RetentionCmdReceipts),
				slog.Duration("consumed_events", cfg.RetentionConsumedEvents),
			)
		} else if retentionPolicy.Enabled() {
			// Load rejects this configuration; keep this guard so a future caller
			// cannot construct time.NewTicker(0) and panic the process.
			log.Error("retention reaper disabled: non-positive interval", slog.Duration("interval", cfg.RetentionInterval))
		} else {
			log.Info("retention reaper disabled (all retention windows are 0)")
		}
		if len(cfg.MySQLShards) > 1 {
			compensators := make([]*socialinfra.FriendEdgeCompensator, 0, len(pools))
			for _, shardCfg := range cfg.MySQLShards {
				compensators = append(compensators, socialinfra.NewFriendEdgeCompensator(pools[shardCfg.Name]))
			}
			go runFriendEdgeCompensationLoop(ctx, compensators, log, time.Minute, cfg.OutboxBatchSize)
		}

		// Kafka consumer runs in a background goroutine when driver=kafka.
		if cfg.EventBusDriver == "kafka" {
			consumer := infrastructure.NewKafkaConsumer(
				cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "projector"),
				dedup, projector, log, clock.System{},
			).WithDedupResolver(dedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
				WithObserver(metrics)
			mailConsumer := infrastructure.NewKafkaConsumer(
				cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "mail"),
				dedup, mailProjector, log, clock.System{},
			).WithDedupResolver(dedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
				WithObserver(metrics)
			taskConsumer := infrastructure.NewKafkaConsumer(
				cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "task"),
				dedup, taskProjector, log, clock.System{},
			).WithDedupResolver(dedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
				WithObserver(metrics)
			catalogConsumer := infrastructure.NewKafkaConsumer(
				cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "catalog"),
				dedup, catalogProjector, log, clock.System{},
			).WithDedupResolver(dedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
				WithObserver(metrics)
			var socialConsumer *infrastructure.KafkaConsumer
			var crossShardStealConsumer *infrastructure.KafkaConsumer
			if len(cfg.MySQLShards) > 1 {
				sagas := make(map[string]*socialinfra.FriendEdgeSaga, len(pools))
				for name, db := range pools {
					sagas[name] = socialinfra.NewFriendEdgeSaga(db)
				}
				socialProjector, projectorErr := socialinfra.NewFriendEdgeProjector(shardRouter, sagas)
				if projectorErr != nil {
					return projectorErr
				}
				socialConsumer = infrastructure.NewKafkaConsumer(
					cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "friend-edge"),
					dedup, socialProjector, log, clock.System{},
				).WithDedupResolver(dedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
					WithObserver(metrics)
				stealProjector, projectorErr := infrastructure.NewCrossShardStealProjector(shardRouter, pools)
				if projectorErr != nil {
					return projectorErr
				}
				stealDedupResolver, resolverErr := infrastructure.NewCrossShardStealDedupResolver(shardRouter, shardDedups, dedup)
				if resolverErr != nil {
					return resolverErr
				}
				crossShardStealConsumer = infrastructure.NewKafkaConsumer(
					cfg.KafkaBrokers, cfg.KafkaTopic, consumerGroup(cfg.KafkaConsumerGroupPrefix, "cross-shard-steal"),
					dedup, stealProjector, log, clock.System{},
				).WithDedupResolver(stealDedupResolver).WithFailurePolicy(cfg.KafkaConsumerMaxTries, cfg.KafkaConsumerRetryMin, cfg.KafkaConsumerRetryMax).
					WithObserver(metrics)
			}
			go func() {
				if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
					log.Error("kafka consumer exited", slog.String("error", err.Error()))
				}
			}()
			go func() {
				if err := mailConsumer.Run(ctx); err != nil && ctx.Err() == nil {
					log.Error("kafka mail consumer exited", slog.String("error", err.Error()))
				}
			}()
			go func() {
				if err := taskConsumer.Run(ctx); err != nil && ctx.Err() == nil {
					log.Error("kafka task consumer exited", slog.String("error", err.Error()))
				}
			}()
			go func() {
				if err := catalogConsumer.Run(ctx); err != nil && ctx.Err() == nil {
					log.Error("kafka catalog consumer exited", slog.String("error", err.Error()))
				}
			}()
			if socialConsumer != nil {
				go func() {
					if err := socialConsumer.Run(ctx); err != nil && ctx.Err() == nil {
						log.Error("kafka friend-edge consumer exited", slog.String("error", err.Error()))
					}
				}()
			}
			if crossShardStealConsumer != nil {
				go func() {
					if err := crossShardStealConsumer.Run(ctx); err != nil && ctx.Err() == nil {
						log.Error("kafka cross-shard steal consumer exited", slog.String("error", err.Error()))
					}
				}()
			}
		}

		log.Info("workersvr ready",
			slog.String("event_bus", cfg.EventBusDriver),
			slog.String("projector", projector.Name()),
		)
		// memory 模式下直接从 MemPublisher.Drain 消费，不走 Kafka。
		if cfg.EventBusDriver != "kafka" {
			if memPub, ok := pub.(*infrastructure.MemPublisher); ok {
				return runLoopMem(ctx, primaryRelay, dedup, mailProjector, taskProjector, catalogProjector, petScanner, memPub, log)
			}
		}
		return runLoopWithPet(ctx, relay, petScanner, log, cfg.OutboxPollInterval, cfg.OutboxDrainInterval, cfg.PetScanInterval, cfg.PetScanIdleMaxBackoff, cfg.PetScanBatchSize)
	}); err != nil {
		log.Error("workersvr exited with error", slog.String("error", err.Error()))
	}
}

// buildPublisher selects the EventPublisher based on EventBusDriver.
func buildPublisher(cfg *config.Common, log *slog.Logger) infrastructure.EventPublisher {
	if cfg.EventBusDriver == "kafka" {
		log.Info("event bus: kafka", slog.String("brokers", cfg.KafkaBrokers))
		return infrastructure.NewKafkaPublisher(cfg.KafkaBrokers)
	}
	log.Info("event bus: memory (MemPublisher)")
	return infrastructure.NewMemPublisher()
}

func consumerGroup(prefix, consumer string) string { return prefix + "-" + consumer }

type outboxScanner interface {
	ScanAndPublish(context.Context) (int, error)
}

type petScanner interface {
	Scan(context.Context, int) (worker.PetScanResult, error)
}

type retentionReaper interface {
	Reap(context.Context) (int, error)
}

// runRetentionLoop 周期性执行保留期清理。清理是尽力而为的维护任务：
// 失败只记录并等下一个 tick，不影响 Relay、消费者或宠物调度。
func runRetentionLoop(ctx context.Context, reaper retentionReaper, log *slog.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if deleted, err := reaper.Reap(ctx); err != nil {
				log.Error("retention reap failed", slog.String("error", err.Error()))
			} else if deleted > 0 {
				log.Info("retention reap completed", slog.Int("deleted", deleted))
			}
		}
	}
}

// runFriendEdgeCompensationLoop only compensates requests whose source Outbox
// reached the terminal DEAD state. It never treats Kafka delay as failure.
func runFriendEdgeCompensationLoop(ctx context.Context, compensators []*socialinfra.FriendEdgeCompensator, log *slog.Logger, interval time.Duration, batch int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, compensator := range compensators {
				count, err := compensator.CompensateDeadRequests(ctx, batch)
				if err != nil {
					log.Error("friend edge compensation failed", slog.String("error", err.Error()))
				} else if count > 0 {
					log.Warn("friend edge requests compensated", slog.Int("count", count))
				}
			}
		}
	}
}

// runLoopWithPet keeps relay draining independent from the pet scheduler. A
// backlog is scanned continuously with a small bounded yield; an empty queue
// uses the configured idle poll interval.
func runLoopWithPet(ctx context.Context, relay outboxScanner, pet petScanner, log *slog.Logger, pollInterval, drainInterval, petInterval, petIdleMaxBackoff time.Duration, petBatch int) error {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runRelayLoop(ctx, relay, log, pollInterval, drainInterval)
	}()
	go func() {
		defer wg.Done()
		runPetLoop(ctx, pet, log, petInterval, petIdleMaxBackoff, petBatch)
	}()
	<-ctx.Done()
	wg.Wait()
	return nil
}

func runRelayLoop(ctx context.Context, relay outboxScanner, log *slog.Logger, pollInterval, drainInterval time.Duration) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			n, err := relay.ScanAndPublish(ctx)
			next := pollInterval
			if err != nil {
				log.Error("outbox scan failed", slog.String("error", err.Error()))
			} else if n > 0 {
				log.Info("outbox published", slog.Int("count", n))
				next = drainInterval
			}
			timer.Reset(next)
		}
	}
}

func runPetLoop(ctx context.Context, pet petScanner, log *slog.Logger, interval, idleMaxBackoff time.Duration, batch int) {
	if idleMaxBackoff < interval {
		idleMaxBackoff = interval
	}
	idle := interval
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			result, err := pet.Scan(ctx, batch)
			next := interval
			if err != nil {
				log.Error("pet scan failed", slog.String("error", err.Error()))
				idle = interval
			} else if result.Claimed > 0 {
				// Claimed work may exceed one batch. Yield briefly, then drain again.
				next = 5 * time.Millisecond
				idle = interval
				if result.Harvested > 0 || result.Rescheduled > 0 || result.SubmitFailed > 0 {
					log.Info("pet scan completed", slog.Int("claimed", result.Claimed), slog.Int("harvested", result.Harvested), slog.Int("rescheduled", result.Rescheduled), slog.Int("submit_failed", result.SubmitFailed))
				}
			} else {
				next = idle
				idle *= 2
				if idle > idleMaxBackoff {
					idle = idleMaxBackoff
				}
			}
			timer.Reset(next)
		}
	}
}

// runLoopMem 是 memory 模式下的 runLoop。
func runLoopMem(
	ctx context.Context,
	relay *infrastructure.OutboxRelay,
	dedup *infrastructure.ConsumerDedup,
	mailProjector infrastructure.EventProjector,
	taskProjector infrastructure.EventProjector,
	catalogProjector infrastructure.EventProjector,
	pet petScanner,
	mem *infrastructure.MemPublisher,
	log *slog.Logger,
) error {
	// Keep pet scheduling independent from the in-memory relay/projector loop as
	// well. Development uses this mode; multi-shard production rejects it.
	go runPetLoop(ctx, pet, log, time.Second, 5*time.Second, 50)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n, err := relay.ScanAndPublish(ctx)
			if err != nil {
				log.Error("outbox scan failed", slog.String("error", err.Error()))
				continue
			}
			if n > 0 {
				log.Info("outbox published", slog.Int("count", n))
			}
			for _, msg := range mem.Drain() {
				now := time.Now().UTC()
				for _, proj := range []infrastructure.EventProjector{mailProjector, taskProjector, catalogProjector} {
					if err := dedup.ProcessOnce(ctx, msg.Payload, proj, now); err != nil {
						log.Error("projector failed",
							slog.String("projector", proj.Name()),
							slog.String("error", err.Error()),
						)
					}
				}
			}
		}
	}
}
