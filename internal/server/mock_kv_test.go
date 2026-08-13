package server

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockRecord struct {
	val []byte
	rev uint64
}

type mockKeyValue struct {
	jetstream.KeyValue
	mu    sync.RWMutex
	store map[string]mockRecord
}

func newMockKeyValue() *mockKeyValue {
	return &mockKeyValue{store: make(map[string]mockRecord)}
}

func (m *mockKeyValue) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = mockRecord{val: value, rev: 1}
	return 1, nil
}

func (m *mockKeyValue) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.store[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return &mockKeyValueEntry{val: rec.val, rev: rec.rev}, nil
}

func (m *mockKeyValue) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.store[key]; !ok {
		return jetstream.ErrKeyNotFound
	}
	delete(m.store, key)
	return nil
}

func (m *mockKeyValue) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.store[key]
	if ok && rec.rev != revision {
		return 0, fmt.Errorf("wrong sequence")
	}
	m.store[key] = mockRecord{val: value, rev: revision + 1}
	return revision + 1, nil
}

func (m *mockKeyValue) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.store) == 0 {
		return nil, jetstream.ErrNoKeysFound
	}
	keys := make([]string, 0, len(m.store))
	for k := range m.store {
		keys = append(keys, k)
	}
	return keys, nil
}

type mockKeyValueEntry struct {
	jetstream.KeyValueEntry
	val []byte
	rev uint64
}

func (e *mockKeyValueEntry) Value() []byte {
	return e.val
}

func (e *mockKeyValueEntry) Revision() uint64 {
	return e.rev
}

type mockJetStreamForServer struct {
	jetstream.JetStream
	kv *mockKeyValue
}

func (m *mockJetStreamForServer) CreateOrUpdateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	if m.kv == nil {
		m.kv = newMockKeyValue()
	}
	return m.kv, nil
}

func countTasks(s *HumanValidationServer) int {
	keys, err := s.kv.Keys(context.Background())
	if err != nil {
		return 0
	}
	return len(keys)
}

func setTaskStatus(s *HumanValidationServer, taskId string, status string) {
	entry, err := s.kv.Get(context.Background(), taskId)
	if err != nil {
		return
	}
	var task pb.PendingTask
	proto.Unmarshal(entry.Value(), &task)
	task.Status = status
	taskBytes, _ := proto.Marshal(&task)
	s.kv.Put(context.Background(), taskId, taskBytes)
}

func getTask(s *HumanValidationServer, taskId string) *pb.PendingTask {
	entry, err := s.kv.Get(context.Background(), taskId)
	if err != nil {
		return nil
	}
	var task pb.PendingTask
	proto.Unmarshal(entry.Value(), &task)
	return &task
}

func unmarshalStruct(jsonStr string) *structpb.Struct {
	var s structpb.Struct
	protojson.Unmarshal([]byte(jsonStr), &s)
	return &s
}

type mockOrchClient struct {
	pb.OrchestratorServiceClient
	resumeJobFunc func(ctx context.Context, in *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error)
}

func (m *mockOrchClient) ResumeJob(ctx context.Context, in *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
	if m.resumeJobFunc != nil {
		return m.resumeJobFunc(ctx, in, opts...)
	}
	return &pb.ResumeJobResponse{Success: true, Message: "Job resumed"}, nil
}
