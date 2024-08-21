package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NeerajNagure/KVstore/keyvaluestore"
)

func TestKeyValueStore(t *testing.T) {
	// Create a key-value store with 2 shards and 1 replica
	kvStore := keyvaluestore.NewKeyValueStore(2, 1)

	kvStore.Set("key1", "value1", 10*time.Second)
	kvStore.Set("key2", "value2", 10*time.Second)

	testGet(t, kvStore, "key1", "value1")
	testGet(t, kvStore, "key2", "value2")

	testGetNonExistent(t, kvStore, "key3")

	testExpiration(t, kvStore, "key1", 10*time.Second)

	testConcurrentAccess(t, kvStore)
}

func testGet(t *testing.T, kvStore *keyvaluestore.KeyValueStore, key, expectedValue string) {
	t.Helper()
	value, exists := kvStore.Get(key)
	if !exists || value != expectedValue {
		t.Errorf("Expected '%s' for key '%s', but got '%s'", expectedValue, key, value)
	}
}

func testGetNonExistent(t *testing.T, kvStore *keyvaluestore.KeyValueStore, key string) {
	t.Helper()
	value, exists := kvStore.Get(key)
	if exists || value != "" {
		t.Errorf("Expected non-existence for key '%s', but got '%s'", key, value)
	}
}

func testExpiration(t *testing.T, kvStore *keyvaluestore.KeyValueStore, key string, ttl time.Duration) {
	t.Helper()

	time.Sleep(ttl + 2*time.Second)

	value, exists := kvStore.Get(key)
	if exists || value != "" {
		t.Errorf("Expected non-existence for expired key '%s', but got '%s'", key, value)
	}
}

func testConcurrentAccess(t *testing.T, kvStore *keyvaluestore.KeyValueStore) {
	t.Helper()

	// Set a key with a longer TTL
	kvStore.Set("key_concurrent", "value_concurrent", 30*time.Second)

	// Use a wait group to synchronize goroutines
	var wg sync.WaitGroup
	// Number of concurrent readers and writers
	numReaders := 5
	numWriters := 5

	// Perform concurrent reads
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, exists := kvStore.Get("key_concurrent")
			if !exists || value != "value_concurrent" {
				t.Errorf("Concurrent read failed. Expected 'value_concurrent', but got '%s'", value)
			}
		}()
	}

	// Perform concurrent writes
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kvStore.Set("key_concurrent_write", fmt.Sprintf("value_concurrent_%d", i), 10*time.Second)
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Allow some time for the writers to complete
	time.Sleep(2 * time.Second)
	finalValue, exists := kvStore.Get("key_concurrent_write")
	fmt.Print("final value", finalValue)
	if !exists || finalValue != "value_concurrent_4" {
		t.Errorf("Concurrent writes did not produce the expected final value. Got '%s'", finalValue)
	}
}
