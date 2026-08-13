// Package app 提供四个部署单元共享的统一生命周期骨架（见 ADR-015）：
// 固定顺序初始化、诊断 HTTP（/live /ready /metrics 占位）、shutdown hook 与优雅退出。
// cmd/<svc>/main.go 只做依赖装配，不含业务规则。
package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ReadinessCheck 返回 nil 表示该依赖已就绪；/ready 聚合所有检查。
// 禁止固定返回 200，必须实际探测 MySQL/Redis/Kafka/Actor Runtime 等真实依赖。
type ReadinessCheck func(ctx context.Context) error

// App 是统一应用容器。
type App struct {
	service         string
	log             *slog.Logger
	diagAddr        string
	shutdownTimeout time.Duration

	mu            sync.RWMutex
	ready         bool
	readyChecks   map[string]ReadinessCheck
	shutdownHooks []func(context.Context) error

	diagServer     *http.Server
	metricsHandler http.Handler
}

// SetMetricsHandler replaces the bootstrap placeholder with the process registry.
func (a *App) SetMetricsHandler(h http.Handler) { a.metricsHandler = h }

// New 构造 App。
func New(service, diagAddr string, log *slog.Logger, shutdownTimeout time.Duration) *App {
	return &App{
		service:         service,
		log:             log,
		diagAddr:        diagAddr,
		shutdownTimeout: shutdownTimeout,
		readyChecks:     make(map[string]ReadinessCheck),
	}
}

// AddReadinessCheck 注册一个就绪检查；名称用于 /ready 明细。
func (a *App) AddReadinessCheck(name string, c ReadinessCheck) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readyChecks[name] = c
}

// AddShutdownHook 注册优雅退出钩子，按注册逆序执行。
func (a *App) AddShutdownHook(fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.shutdownHooks = append(a.shutdownHooks, fn)
}

// MarkReady 在依赖装配完成后置为可接收流量。
func (a *App) MarkReady() {
	a.mu.Lock()
	a.ready = true
	a.mu.Unlock()
}

// markNotReady 在 drain 时先摘流量。
func (a *App) markNotReady() {
	a.mu.Lock()
	a.ready = false
	a.mu.Unlock()
}

func (a *App) handleLive(w http.ResponseWriter, _ *http.Request) {
	// 进程存活即返回 200；不代表依赖就绪。
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"live"}`))
}

func (a *App) handleReady(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	ready := a.ready
	checks := make(map[string]ReadinessCheck, len(a.readyChecks))
	for k, v := range a.readyChecks {
		checks[k] = v
	}
	a.mu.RUnlock()

	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"draining"}`))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for name, c := range checks {
		if err := c(ctx); err != nil {
			a.log.Warn("readiness check failed", slog.String("check", name), slog.String("error", err.Error()))
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"degraded","failed":"` + name + `"}`))
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (a *App) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if a.metricsHandler == nil {
		http.Error(w, "metrics registry not configured", http.StatusServiceUnavailable)
		return
	}
	a.metricsHandler.ServeHTTP(w, r)
}

// Run 启动诊断服务并阻塞到收到 SIGINT/SIGTERM，然后按逆序执行 shutdown hook。
// serve 是各服务的主监听循环（阻塞式），随 ctx 取消退出。
func (a *App) Run(serve func(ctx context.Context) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/live", a.handleLive)
	mux.HandleFunc("/ready", a.handleReady)
	mux.HandleFunc("/metrics", a.handleMetrics)
	a.diagServer = &http.Server{Addr: a.diagAddr, Handler: mux}

	go func() {
		a.log.Info("diag server listening", slog.String("addr", a.diagAddr))
		if err := a.diagServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("diag server error", slog.String("error", err.Error()))
		}
	}()

	serveErr := make(chan error, 1)
	go func() { serveErr <- serve(ctx) }()

	select {
	case <-ctx.Done():
		a.log.Info("shutdown signal received")
	case err := <-serveErr:
		if err != nil {
			a.log.Error("serve exited with error", slog.String("error", err.Error()))
		}
	}

	return a.shutdown()
}

// shutdown 先摘流量，再逆序执行 hook，最后关闭诊断服务。
func (a *App) shutdown() error {
	a.markNotReady()
	ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()

	a.mu.RLock()
	hooks := make([]func(context.Context) error, len(a.shutdownHooks))
	copy(hooks, a.shutdownHooks)
	a.mu.RUnlock()

	var firstErr error
	for i := len(hooks) - 1; i >= 0; i-- {
		if err := hooks[i](ctx); err != nil {
			a.log.Error("shutdown hook error", slog.String("error", err.Error()))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if a.diagServer != nil {
		_ = a.diagServer.Shutdown(ctx)
	}
	a.log.Info("shutdown complete")
	return firstErr
}

// Getenv 便于 main 读取非关键可选变量。
func Getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
