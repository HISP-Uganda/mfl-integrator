package models

import "sync/atomic"

type serverRegistrySnapshot struct {
	byID   map[string]Server
	byName map[string]Server
}

var serverRegistry atomic.Value // stores serverRegistrySnapshot

// ReplaceServerRegistry swaps the entire in-memory registry atomically.
// Callers should treat the provided maps as immutable after calling this.
func ReplaceServerRegistry(byID map[string]Server, byName map[string]Server) {
	if byID == nil {
		byID = map[string]Server{}
	}
	if byName == nil {
		byName = map[string]Server{}
	}
	serverRegistry.Store(serverRegistrySnapshot{
		byID:   byID,
		byName: byName,
	})
}

func GetServerFromRegistryByID(id string) (Server, bool) {
	s := serverRegistry.Load().(serverRegistrySnapshot)
	v, ok := s.byID[id]
	return v, ok
}

func GetServerFromRegistryByName(name string) (Server, bool) {
	s := serverRegistry.Load().(serverRegistrySnapshot)
	v, ok := s.byName[name]
	return v, ok
}
