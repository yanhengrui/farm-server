// Package id 生成命令与票据用的 128-bit 标识（UUIDv7 风格，时间有序）。
// 实体 ID（user_id/farm_id/mail_id）由 MySQL BIGINT/号段负责，不在此生成。
// 见 ADR-009 / 02-数据模型与迁移设计.md。
package id

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// NewV7 返回时间有序的 128-bit 十六进制字符串（无连字符），
// 用作 cmd_id、invite_token、resume_ticket 等；写入 MySQL 时建唯一索引。
func NewV7() string {
	var b [16]byte
	// 前 48 位为毫秒时间戳，保证大致有序，减少 B+ 树随机写。
	ms := uint64(time.Now().UTC().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	_, _ = rand.Read(b[6:])
	// 版本位与变体位（UUIDv7 语义）。
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:])
}

// NormalizeV7 validates a UUIDv7 in compact (32 hex) or canonical (36 chars)
// form and returns the compact lowercase representation used by MySQL UNHEX.
func NormalizeV7(value string) (string, bool) {
	compact := strings.ToLower(strings.TrimSpace(value))
	if len(compact) == 36 {
		if compact[8] != '-' || compact[13] != '-' || compact[18] != '-' || compact[23] != '-' {
			return "", false
		}
		compact = strings.ReplaceAll(compact, "-", "")
	}
	if len(compact) != 32 || compact[12] != '7' || !strings.ContainsRune("89ab", rune(compact[16])) {
		return "", false
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return "", false
	}
	return compact, true
}
