package keyvaluestore

import (
	"sync"
	"time"
)

type KeyValue struct {
	Value      string
	Expiration time.Time
}

type Shard struct {
	data map[string]KeyValue
	mu   sync.RWMutex
}

type KeyValueStore struct {
	data      map[string]KeyValue
	mu        map[string]*sync.RWMutex
	muControl sync.RWMutex
	shards    []*Shard
	replicas  int
}

func fnvHash(data string) uint32 {
	const prime = 16777619
	hash := uint32(2166136261)

	for i := 0; i < len(data); i++ {
		hash ^= uint32(data[i])
		hash *= prime
	}

	return hash
}

func (kv *KeyValueStore) GetShardIndex(key string) int {
	hash := fnvHash(key)
	return int(hash) % len(kv.shards)
}

func NewKeyValueStore(numShards, numReplicas int) *KeyValueStore {
	store := &KeyValueStore{
		shards:   make([]*Shard, numShards),
		replicas: numReplicas,
	}

	for i := 0; i < numShards; i++ {
		store.shards[i] = &Shard{data: make(map[string]KeyValue)}
	}

	return store
}

func (kv *KeyValueStore) getLock(key string) *sync.RWMutex {
	kv.muControl.Lock()
	defer kv.muControl.Unlock()

	if kv.mu == nil {
		kv.mu = make(map[string]*sync.RWMutex)
	}

	if _, ok := kv.mu[key]; !ok {
		kv.mu[key] = &sync.RWMutex{}
	}

	return kv.mu[key]
}

func (kv *KeyValueStore) Set(key, value string, ttl time.Duration) {
	shardIndex := kv.GetShardIndex(key)
	shard := kv.shards[shardIndex]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	kv.getLock(key).Lock()
	defer kv.getLock(key).Unlock()

	expiration := time.Now().Add(ttl)
	shard.data[key] = KeyValue{
		Value:      value,
		Expiration: expiration,
	}
}

func (kv *KeyValueStore) Get(key string) (string, bool) {
    shardIndex := kv.GetShardIndex(key)
    shard := kv.shards[shardIndex]
    shard.mu.RLock()
    defer shard.mu.RUnlock()
	kv.getLock(key).RLock()
	defer kv.getLock(key).RUnlock()

	item, ok := shard.data[key]
	if !ok {
		return "", false
	}
	if item.Expiration.IsZero() || time.Now().Before(item.Expiration) {
		return item.Value, true
	}
	delete(kv.data, key)
	return "", false
}
