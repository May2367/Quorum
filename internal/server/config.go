package server

import (
	"net/http"
	"sync/atomic"
)

type Config struct {
	NodeID     uint64
	HTTPAddr   string
	ClientAddr string
	PeerAddr   string
	DataDir    string
	Peers      []string
}

type HealthServer struct {
	ready atomic.Bool
	mux   *http.ServeMux
}

func NewHealthServer() *HealthServer {
	h := &HealthServer{mux: http.NewServeMux()}
	h.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	h.mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !h.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return h
}

func (h *HealthServer) SetReady(ready bool) {
	h.ready.Store(ready)
}

func (h *HealthServer) Handler() http.Handler {
	return h.mux
}
