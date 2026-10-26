package server

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type Config struct {
	NodeID           uint64
	HTTPAddr         string
	ClientAddr       string
	PeerAddr         string
	DataDir          string
	PeerMap          string
	ElectionTimeout  int
	HeartbeatTimeout int
}

type PeerConfig struct {
	IDs   []uint64
	Addrs map[uint64]string
}

func ParsePeerMap(raw string, selfID uint64, selfAddr string) (PeerConfig, error) {
	addrs := map[uint64]string{}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return PeerConfig{IDs: []uint64{selfID}, Addrs: map[uint64]string{selfID: selfAddr}}, nil
	}

	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			return PeerConfig{}, fmt.Errorf("invalid peer %q, want id=addr", item)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil || id == 0 {
			return PeerConfig{}, fmt.Errorf("invalid peer id %q", parts[0])
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return PeerConfig{}, fmt.Errorf("peer %d address is empty", id)
		}
		if _, exists := addrs[id]; exists {
			return PeerConfig{}, fmt.Errorf("duplicate peer id %d", id)
		}
		addrs[id] = addr
	}
	if len(addrs) == 0 {
		return PeerConfig{}, fmt.Errorf("peer map is empty")
	}
	if _, ok := addrs[selfID]; !ok {
		if strings.TrimSpace(selfAddr) == "" {
			return PeerConfig{}, fmt.Errorf("peer map does not include local node %d", selfID)
		}
		addrs[selfID] = selfAddr
	}

	ids := make([]uint64, 0, len(addrs))
	for id := range addrs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return PeerConfig{IDs: ids, Addrs: addrs}, nil
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
