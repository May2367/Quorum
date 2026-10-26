package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthServerReadiness(t *testing.T) {
	h := NewHealthServer()
	resp := httptest.NewRecorder()
	h.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial ready status = %d", resp.Code)
	}
	h.SetReady(true)
	resp = httptest.NewRecorder()
	h.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("ready status = %d", resp.Code)
	}
}

func TestParsePeerMap(t *testing.T) {
	cfg, err := ParsePeerMap("1=127.0.0.1:9001,2=127.0.0.1:9002", 1, ":9001")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.IDs) != 2 || cfg.IDs[0] != 1 || cfg.IDs[1] != 2 {
		t.Fatalf("ids = %+v, want [1 2]", cfg.IDs)
	}
	if cfg.Addrs[2] != "127.0.0.1:9002" {
		t.Fatalf("peer 2 addr = %q", cfg.Addrs[2])
	}
}

func TestParsePeerMapAddsLocalNodeWhenMissing(t *testing.T) {
	cfg, err := ParsePeerMap("2=127.0.0.1:9002", 1, "127.0.0.1:9001")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.IDs) != 2 || cfg.IDs[0] != 1 || cfg.IDs[1] != 2 {
		t.Fatalf("ids = %+v, want [1 2]", cfg.IDs)
	}
	if cfg.Addrs[1] != "127.0.0.1:9001" {
		t.Fatalf("local addr = %q", cfg.Addrs[1])
	}
}

func TestParsePeerMapRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{"1", "x=127.0.0.1:9001", "1=addr,1=other", "1="} {
		if _, err := ParsePeerMap(raw, 1, ":9001"); err == nil {
			t.Fatalf("ParsePeerMap(%q) succeeded, want error", raw)
		}
	}
}
