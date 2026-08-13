package id

import "testing"

func TestNormalizeV7(t *testing.T) {
	compact := NewV7()
	if got, ok := NormalizeV7(compact); !ok || got != compact {
		t.Fatalf("compact got=%q ok=%v", got, ok)
	}
	canonical := compact[:8] + "-" + compact[8:12] + "-" + compact[12:16] + "-" + compact[16:20] + "-" + compact[20:]
	if got, ok := NormalizeV7(canonical); !ok || got != compact {
		t.Fatalf("canonical got=%q ok=%v", got, ok)
	}
	for _, invalid := range []string{"", "cmd-1", compact[:12] + "4" + compact[13:], compact[:16] + "7" + compact[17:]} {
		if _, ok := NormalizeV7(invalid); ok {
			t.Fatalf("accepted invalid UUIDv7 %q", invalid)
		}
	}
}
