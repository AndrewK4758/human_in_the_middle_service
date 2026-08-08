package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type mockKeyValue struct {
	jetstream.KeyValue
	mu   sync.RWMutex
	data map[string]mockKVEntry
}

type mockKVEntry struct {
	val []byte
	rev uint64
}

func newMockKeyValue() *mockKeyValue {
	return &mockKeyValue{
		data: make(map[string]mockKVEntry),
	}
}

func (m *mockKeyValue) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.data[key]
	entry.val = value
	entry.rev++
	m.data[key] = entry
	return entry.rev, nil
}

func (m *mockKeyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return &mockKVEntryImpl{key: key, val: entry.val, rev: entry.rev}, nil
}

func (m *mockKeyValue) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.data[key]
	if ok && entry.rev != revision {
		return 0, fmt.Errorf("wrong revision")
	}
	entry.val = value
	entry.rev++
	m.data[key] = entry
	return entry.rev, nil
}

func (m *mockKeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; !ok {
		return jetstream.ErrKeyNotFound
	}
	delete(m.data, key)
	return nil
}

func (m *mockKeyValue) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, jetstream.ErrNoKeysFound
	}
	return keys, nil
}

type mockKVEntryImpl struct {
	key string
	val []byte
	rev uint64
}

func (e *mockKVEntryImpl) Bucket() string       { return "MOCK" }
func (e *mockKVEntryImpl) Key() string          { return e.key }
func (e *mockKVEntryImpl) Value() []byte        { return e.val }
func (e *mockKVEntryImpl) Revision() uint64     { return e.rev }
func (e *mockKVEntryImpl) Created() time.Time   { return time.Now() }
func (e *mockKVEntryImpl) Delta() uint64        { return 0 }
func (e *mockKVEntryImpl) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }

func (m *mockJetStream) CreateOrUpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return newMockKeyValue(), nil
}

func (m *mockJetStream) CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return newMockKeyValue(), nil
}

func (m *mockJetStream) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	return newMockKeyValue(), nil
}

func (m *mockJetStream) DeleteKeyValue(ctx context.Context, bucket string) error { return nil }
