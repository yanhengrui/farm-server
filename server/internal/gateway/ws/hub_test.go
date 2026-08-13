package ws

import (
	"encoding/json"
	"testing"
	"time"

	wscontract "github.com/photon/farm-server/server/contracts/ws"
)

func makeTestConn() *connState {
	return &connState{
		writeCh: make(chan []byte, writeChanCap),
		done:    make(chan struct{}),
	}
}

func marshalFrame(t *testing.T, meta wscontract.Meta) []byte {
	t.Helper()
	b, _ := json.Marshal(wscontract.Frame{Meta: meta})
	return b
}

// TestHub_Subscribe_Basic 验证订阅后 FarmViewerCount 正确。
func TestHub_Subscribe_Basic(t *testing.T) {
	h := NewHub()
	c := makeTestConn()
	if err := h.Subscribe(1001, 0, c); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if h.FarmViewerCount(1001) != 1 {
		t.Errorf("expected 1 viewer, got %d", h.FarmViewerCount(1001))
	}
}

// TestHub_Subscribe_ResubscribeMovesConn 验证重新订阅时自动离开旧农场。
func TestHub_Subscribe_ResubscribeMovesConn(t *testing.T) {
	h := NewHub()
	c := makeTestConn()
	if err := h.Subscribe(1001, 0, c); err != nil {
		t.Fatalf("subscribe first farm: %v", err)
	}
	if err := h.Subscribe(2002, 0, c); err != nil {
		t.Fatalf("subscribe second farm: %v", err)
	}
	if h.FarmViewerCount(1001) != 0 {
		t.Errorf("expected 0 viewers for farm 1001 after resubscribe, got %d", h.FarmViewerCount(1001))
	}
	if h.FarmViewerCount(2002) != 1 {
		t.Errorf("expected 1 viewer for farm 2002, got %d", h.FarmViewerCount(2002))
	}
}

func TestHub_Subscribe_FullTargetKeepsOldFarm(t *testing.T) {
	h := NewHub()
	viewer := makeTestConn()
	viewer.userID = 9001
	if err := h.Subscribe(9001, 7, viewer); err != nil {
		t.Fatalf("subscribe old farm: %v", err)
	}
	for i := 0; i < maxGuestViewers; i++ {
		guest := makeTestConn()
		guest.userID = int64(2000 + i)
		if err := h.Subscribe(1001, 3, guest); err != nil {
			t.Fatalf("fill target farm: %v", err)
		}
	}
	if err := h.Subscribe(1001, 3, viewer); err != ErrFarmFull {
		t.Fatalf("expected ErrFarmFull, got %v", err)
	}
	if got := h.FarmViewerCount(9001); got != 1 {
		t.Fatalf("old farm subscription lost: viewers=%d", got)
	}
	if viewer.farmID.Load() != 9001 || viewer.farmVersion.Load() != 7 {
		t.Fatalf("old cursor changed: farm=%d version=%d", viewer.farmID.Load(), viewer.farmVersion.Load())
	}
}

// TestHub_Unsubscribe 验证 Unsubscribe 后 viewer count 降为 0。
func TestHub_Unsubscribe(t *testing.T) {
	h := NewHub()
	c := makeTestConn()
	if err := h.Subscribe(1001, 0, c); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	h.Unsubscribe(c)
	if h.FarmViewerCount(1001) != 0 {
		t.Errorf("expected 0 viewers after unsubscribe, got %d", h.FarmViewerCount(1001))
	}
}

// TestHub_Broadcast_Delivers 验证广播帧送达目标连接。
func TestHub_Broadcast_Delivers(t *testing.T) {
	h := NewHub()
	c1 := makeTestConn()
	c2 := makeTestConn()
	if err := h.Subscribe(1001, 0, c1); err != nil {
		t.Fatalf("subscribe first connection: %v", err)
	}
	if err := h.Subscribe(1001, 0, c2); err != nil {
		t.Fatalf("subscribe second connection: %v", err)
	}

	frame := marshalFrame(t, wscontract.Meta{Type: wscontract.FrameTypeEvent})
	h.Broadcast(1001, frame, nil)

	for _, c := range []*connState{c1, c2} {
		select {
		case got := <-c.writeCh:
			if string(got) != string(frame) {
				t.Errorf("unexpected frame content")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("expected frame not received within timeout")
		}
	}
}

// TestHub_Broadcast_ExcludesExcept 验证 except 连接不收到广播。
func TestHub_Broadcast_ExcludesExcept(t *testing.T) {
	h := NewHub()
	operator := makeTestConn()
	viewer := makeTestConn()
	if err := h.Subscribe(1001, 0, operator); err != nil {
		t.Fatalf("subscribe operator: %v", err)
	}
	if err := h.Subscribe(1001, 0, viewer); err != nil {
		t.Fatalf("subscribe viewer: %v", err)
	}

	frame := marshalFrame(t, wscontract.Meta{Type: wscontract.FrameTypeEvent})
	h.Broadcast(1001, frame, operator) // exclude operator

	select {
	case <-operator.writeCh:
		t.Error("operator should not receive its own broadcast")
	case <-time.After(30 * time.Millisecond):
		// correct: operator not notified
	}

	select {
	case <-viewer.writeCh:
		// correct: viewer received it
	case <-time.After(100 * time.Millisecond):
		t.Error("viewer should have received the broadcast")
	}
}

// TestHub_Broadcast_DropWhenFull 验证 writeCh 满时广播不阻塞（丢弃）。
func TestHub_Broadcast_DropWhenFull(t *testing.T) {
	h := NewHub()
	c := makeTestConn()
	if err := h.Subscribe(1001, 0, c); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Fill the write channel.
	frame := marshalFrame(t, wscontract.Meta{Type: wscontract.FrameTypeEvent})
	for range writeChanCap {
		c.writeCh <- frame
	}

	// This should not block even though channel is full.
	done := make(chan struct{})
	go func() {
		h.Broadcast(1001, frame, nil)
		close(done)
	}()

	select {
	case <-done:
		// correct: broadcast completed without blocking
	case <-time.After(100 * time.Millisecond):
		t.Error("broadcast blocked on full write channel")
	}
}
