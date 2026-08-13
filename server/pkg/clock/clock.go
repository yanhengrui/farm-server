// Package clock 提供可替换的时间源，便于测试确定性与作物成长惰性计算。
// 领域层禁止直接调用 time.Now()，统一通过 Clock 获取服务端权威时间（UTC）。
package clock

import "time"

// Clock 抽象服务端时间源。所有时间以 UTC 返回。
type Clock interface {
	NowUTC() time.Time
}

// System 使用真实系统时间。
type System struct{}

// NowUTC 返回当前 UTC 时间。
func (System) NowUTC() time.Time { return time.Now().UTC() }

// Fixed 用于测试的固定时钟，可推进。
type Fixed struct{ t time.Time }

// NewFixed 构造固定时钟。
func NewFixed(t time.Time) *Fixed { return &Fixed{t: t.UTC()} }

// NowUTC 返回固定时间。
func (f *Fixed) NowUTC() time.Time { return f.t }

// Advance 推进固定时钟。
func (f *Fixed) Advance(d time.Duration) { f.t = f.t.Add(d) }
