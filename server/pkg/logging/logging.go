// Package logging 提供统一结构化日志。禁止记录 Secret/Token/完整 DSN。
// 固定字段见 06-仓库结构与编码规范.md 4.5。
package logging

import (
	"log/slog"
	"os"
)

// New 构造带 service/instance/env 固定字段的 JSON 结构化 logger。
func New(service, instanceID, env, level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With(
		slog.String("service", service),
		slog.String("instance_id", instanceID),
		slog.String("env", env),
	)
}
