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
