package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const connTTL = 90 * time.Second

// ConnStore 维护每用户全局单 WebSocket 连接。
// 键 ws:conn:{user_id} 存 instanceID:sessionID，带 TTL，心跳续期。
// 新连接注册时通过 PubSub 频道通知旧连接主动断开（1005 被踢）。
type ConnStore struct {
	rdb        *redis.Client
	instanceID string
	mu         sync.Mutex
	pubsub     *redis.PubSub
	listeners  map[int64]map[string]func()
}

// NewConnStore 构造 ConnStore。
func NewConnStore(rdb *redis.Client, instanceID string) *ConnStore {
	return &ConnStore{rdb: rdb, instanceID: instanceID, listeners: make(map[int64]map[string]func())}
}

func connKey(userID int64) string { return fmt.Sprintf("ws:conn:%d", userID) }

// KickChannel 返回踢人 PubSub 频道名（供外部订阅）。
func KickChannel(instanceID string) string { return "ws:kick:instance:" + instanceID }

// Start creates the single shared kick subscription for this gatesvr instance.
func (c *ConnStore) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.pubsub != nil {
		c.mu.Unlock()
		return nil
	}
	c.pubsub = c.rdb.Subscribe(ctx, KickChannel(c.instanceID))
	pubsub := c.pubsub
	c.mu.Unlock()
	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = pubsub.Close()
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}
				parts := strings.SplitN(msg.Payload, "|", 2)
				if len(parts) != 2 {
					continue
				}
				userID, err := strconv.ParseInt(parts[0], 10, 64)
				if err != nil {
					continue
				}
				newParts := strings.SplitN(parts[1], ":", 2)
				if len(newParts) != 2 {
					continue
				}
				newSession := newParts[1]
				c.mu.Lock()
				callbacks := make([]func(), 0, len(c.listeners[userID]))
				for sessionID, callback := range c.listeners[userID] {
					if sessionID != newSession {
						callbacks = append(callbacks, callback)
					}
				}
				c.mu.Unlock()
				for _, callback := range callbacks {
					callback()
				}
			}
		}
	}()
	return nil
}

// Register 注册新连接并向频道发布踢人消息（旧连接订阅者收到后关闭自身）。
// val 格式 "instanceID:sessionID" 存入 Redis，并作为 PubSub 消息内容。
func (c *ConnStore) Register(ctx context.Context, userID int64, sessionID string) error {
	val := c.instanceID + ":" + sessionID
	return registerConnScript.Run(ctx, c.rdb, []string{connKey(userID)}, val, strconv.FormatInt(userID, 10), "ws:kick:instance:", int64(connTTL/time.Second)).Err()
}

var registerConnScript = redis.NewScript(`
local old = redis.call("GET", KEYS[1])
redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[4])
if old and old ~= ARGV[1] then
  local sep = string.find(old, ":", 1, true)
  if sep then
    local old_instance = string.sub(old, 1, sep - 1)
    redis.call("PUBLISH", ARGV[3] .. old_instance, ARGV[2] .. "|" .. ARGV[1])
  end
end
return 1
`)

// Refresh 续期连接 TTL（心跳 ping 时调用，间隔应 < connTTL）。
func (c *ConnStore) Refresh(ctx context.Context, userID int64, sessionID string) error {
	expected := c.instanceID + ":" + sessionID
	return refreshConnScript.Run(ctx, c.rdb, []string{connKey(userID)}, expected, int64(connTTL/time.Second)).Err()
}

var refreshConnScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// Unregister 仅当值匹配时删除键，防止误删新连接记录。
var unregisterScript = redis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if cur == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
`)

// Unregister 删除连接记录（仅值匹配时生效）。
func (c *ConnStore) Unregister(ctx context.Context, userID int64, sessionID string) error {
	expected := c.instanceID + ":" + sessionID
	return unregisterScript.Run(ctx, c.rdb, []string{connKey(userID)}, expected).Err()
}

// ListenKick 在后台订阅踢人频道，收到不属于 sessionID 的消息时调用 onKick。
// 直到 done 关闭或 PubSub 断连时自动退出，goroutine 完成后关闭返回的 stopped channel。
// 必须在 Register 之前订阅，以避免漏接踢人消息。
func (c *ConnStore) ListenKick(
	ctx context.Context,
	userID int64,
	sessionID string,
	done <-chan struct{},
	onKick func(),
) (stopped <-chan struct{}) {
	ch := make(chan struct{})
	c.mu.Lock()
	if c.listeners[userID] == nil {
		c.listeners[userID] = make(map[string]func())
	}
	c.listeners[userID][sessionID] = onKick
	c.mu.Unlock()
	go func() {
		defer close(ch)
		select {
		case <-done:
		case <-ctx.Done():
		}
		c.mu.Lock()
		delete(c.listeners[userID], sessionID)
		if len(c.listeners[userID]) == 0 {
			delete(c.listeners, userID)
		}
		c.mu.Unlock()
	}()
	return ch
}
