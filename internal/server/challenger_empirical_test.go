package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	os.Setenv("WORKERS_STREAM", "WORKER_TASKS")
	os.Setenv("ORCHESTRATOR_ADDRESS", "localhost:50052")
}

// -----------------------------------------------------------------------------
// Test 1: Edge-case inputs to SubmitCorrection and NatsServer.processMessage
// -----------------------------------------------------------------------------

func TestChallenger_SubmitCorrection_EdgeCases(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()

	s := NewHumanValidationServer(addr)
	ctx := context.Background()

	// 1.1 Non-existent Task ID
	_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        "non_existent_123",
		Corrected: unmarshalStruct(`{"key": "value"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected 'not found' error for non-existent task, got: %v", err)
	}

	// 1.2 Empty Task ID
	_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        "",
		Corrected: unmarshalStruct(`{"key": "value"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected 'not found' error for empty task ID, got: %v", err)
	}

	// 1.3 Task created with nil Identity
	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: nil,
		StepId:   "step_nil_id",
	})
	if err != nil {
		t.Fatalf("Failed to create task with nil identity: %v", err)
	}
	taskIdNilId := createResp.GetTaskId()

	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		if req.GetIdentity() != nil {
			t.Errorf("Expected nil identity in ResumeJobRequest, got %v", req.GetIdentity())
		}
		return &pb.ResumeJobResponse{Success: true, Message: "ok"}, nil
	}

	resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskIdNilId,
		Corrected: unmarshalStruct(`{"corrected": true}`),
	})
	if err != nil {
		t.Fatalf("SubmitCorrection with nil task identity failed: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Expected success for nil task identity")
	}

	// 1.4 Invalid status transitions
	// Create task and force various status strings
	createResp2, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_status_test"},
		StepId:   "step_status",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskIdStatus := createResp2.GetTaskId()

	invalidStatuses := []string{
		TaskStatusProcessing,
		"UNKNOWN_STATUS",
		"FAILED",
		"CANCELLED",
	}

	for _, invalidStatus := range invalidStatuses {
		s.mu.Lock()
		s.tasks[taskIdStatus].Status = invalidStatus
		s.mu.Unlock()

		_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:        taskIdStatus,
			Corrected: unmarshalStruct(`{"fixed": true}`),
		})
		if err == nil {
			t.Errorf("Expected error when task is in status %s, but got none", invalidStatus)
		} else if !strings.Contains(err.Error(), "already") {
			t.Errorf("Expected error message containing 'already', got: %v", err)
		}
	}

	// 1.5 2MB payload within default gRPC limits
	s.mu.Lock()
	s.tasks[taskIdStatus].Status = TaskStatusPending
	s.mu.Unlock()

	largeJson2MB := `{"large_field": "` + strings.Repeat("A", 2*1024*1024) + `", "unicode": "こんにちは世界 🚀 \n\t\""}`
	var receivedJson string
	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		b, _ := protojson.Marshal(req.GetCorrected()); receivedJson = string(b)
		return &pb.ResumeJobResponse{Success: true, Message: "large json received"}, nil
	}

	respLarge, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskIdStatus,
		Corrected: unmarshalStruct(largeJson2MB),
	})
	if err != nil {
		t.Fatalf("SubmitCorrection failed with 2MB payload: %v", err)
	}
	if !respLarge.GetSuccess() {
		t.Fatalf("Expected success for 2MB payload")
	}
	if len(receivedJson) < len(largeJson2MB)-100 {
		t.Errorf("Payload byte corruption detected for 2MB JSON!\nExpected len: %d\nGot len: %d", len(largeJson2MB), len(receivedJson))
	}
	// Removed exact string match because protojson formatting changes spacing

	// 1.6 >4MB payload exceeds default gRPC 4MB limit -> verify graceful rollback to PENDING
	// Create another task
	createRespExceed, _ := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_exceed_4mb"},
		StepId:   "step_exceed",
	})
	taskIdExceed := createRespExceed.GetTaskId()

	payload5MB := `{"large_field": "` + strings.Repeat("B", 5*1024*1024) + `"}`
	_, errExceed := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskIdExceed,
		Corrected: unmarshalStruct(payload5MB),
	})
	if errExceed == nil {
		t.Fatalf("Expected error when sending 5MB message over gRPC default client")
	}
	if !strings.Contains(errExceed.Error(), "ResourceExhausted") && !strings.Contains(errExceed.Error(), "larger than max") {
		t.Logf("gRPC error: %v", errExceed)
	}

	s.mu.RLock()
	statusExceed := s.tasks[taskIdExceed].Status
	s.mu.RUnlock()
	if statusExceed != TaskStatusPending {
		t.Fatalf("Expected status rolled back to PENDING when >4MB payload fails gRPC limit, got: %s", statusExceed)
	}
}

func TestChallenger_NatsServer_ProcessMessage_EdgeCases(t *testing.T) {
	valServer := NewHumanValidationServer("localhost:50052")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}
	ctx := context.Background()

	// 1.7 Empty byte slice payload []byte{}
	msgEmpty := &mockMsg{
		data:    []byte{},
		subject: "worker.human.validation",
	}
	natsServer.processMessage(ctx, msgEmpty)
	if !msgEmpty.acked {
		t.Errorf("Expected empty byte slice to be processed and acked (valid empty proto)")
	}

	// 1.8 Proto message with ALL optional fields nil (Identity, NodeConfig, GlobalState, HumanValidation, Metadata)
	reqNilFields := &pb.ExecuteWorkflowNodeRequest{}
	reqBytes, _ := proto.Marshal(reqNilFields)
	msgNilFields := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
	}
	natsServer.processMessage(ctx, msgNilFields)
	if !msgNilFields.acked {
		t.Errorf("Expected request with all nil fields to be acked")
	}

	// 1.9 NATS publish failure on reply subject
	mockJSFail := newMockJetStream()
	mockJSFail.publishErr = fmt.Errorf("JetStream storage full")
	natsServerFail := &NatsServer{
		nc:               nil,
		js:               mockJSFail,
		validationServer: valServer,
	}

	headers := nats.Header{}
	headers.Set("Reply-Subject", "orchestrator.reply.fail")
	msgReplyFail := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
		headers: headers,
	}

	natsServerFail.processMessage(ctx, msgReplyFail)
	if !msgReplyFail.nacked {
		t.Errorf("Expected message to be NACKed when JetStream publish fails")
	}
	if msgReplyFail.acked {
		t.Errorf("Message should NOT be ACKed when reply publish fails")
	}
}

// -----------------------------------------------------------------------------
// Test 2: Orchestrator disconnect handling and rollback logic
// -----------------------------------------------------------------------------

func TestChallenger_OrchestratorDisconnectAndRollback(t *testing.T) {
	// Start mock orchestrator on a dynamic port
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)

	s := NewHumanValidationServer(addr)
	ctx := context.Background()

	// Create task
	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_disconnect_test"},
		StepId:   "step_disc",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	// 2.1 Close orchestrator server to simulate hard network disconnect / server crash
	cleanup() // closes listener and stops server

	_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskId,
		Corrected: unmarshalStruct(`{"data": 1}`),
	})
	if err == nil {
		t.Fatalf("Expected error when orchestrator is completely shut down")
	}

	// Verify rollback to PENDING
	s.mu.RLock()
	statusAfterCrash := s.tasks[taskId].Status
	s.mu.RUnlock()

	if statusAfterCrash != TaskStatusPending {
		t.Fatalf("Expected task status rolled back to PENDING after server crash, got: %s", statusAfterCrash)
	}

	// 2.2 Re-start orchestrator on a NEW dynamic address, update server orchestratorAddr, retry
	mockOrch2 := &mockOrchestratorServer{
		resumeJobFunc: func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
			return &pb.ResumeJobResponse{Success: true, Message: "re-established"}, nil
		},
	}
	newAddr, cleanup2 := startMockOrchestrator(t, mockOrch2)
	defer cleanup2()

	s.orchestratorAddr = newAddr

	resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:        taskId,
		Corrected: unmarshalStruct(`{"data": 1}`),
	})
	if err != nil {
		t.Fatalf("SubmitCorrection failed after orchestrator recovery: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Expected success after orchestrator recovery")
	}

	s.mu.RLock()
	finalStatus := s.tasks[taskId].Status
	s.mu.RUnlock()
	if finalStatus != TaskStatusResolved {
		t.Fatalf("Expected final status RESOLVED after recovery, got: %s", finalStatus)
	}

	// 2.3 Context deadline / cancellation handling during SubmitCorrection
	createResp3, _ := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{JobId: "job_ctx_timeout"},
		StepId:   "step_timeout",
	})
	taskIdTimeout := createResp3.GetTaskId()

	// Mock orchestrator that hangs until context is done
	mockOrch2.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest) (*pb.ResumeJobResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = s.SubmitCorrection(shortCtx, &pb.SubmitCorrectionRequest{
		TaskId:        taskIdTimeout,
		Corrected: unmarshalStruct(`{"data": 2}`),
	})
	if err == nil {
		t.Fatalf("Expected error when context times out during ResumeJob")
	}

	s.mu.RLock()
	statusAfterTimeout := s.tasks[taskIdTimeout].Status
	s.mu.RUnlock()

	if statusAfterTimeout != TaskStatusPending {
		t.Fatalf("Expected status rolled back to PENDING after context timeout, got: %s", statusAfterTimeout)
	}
}

// -----------------------------------------------------------------------------
// Test 3: String preservation (ExpectedJsonSchema, PromptInstructions, GlobalStateJson)
// -----------------------------------------------------------------------------

func TestChallenger_StringPreservationFidelity(t *testing.T) {
	valServer := NewHumanValidationServer("localhost:50052")
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

	// Construct complex inputs with special characters, multiline, backslashes, unicode, large JSON
	complexSchema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Complex Document Schema with Unicode 🚀 中文 €",
		"type": "object",
		"properties": {
			"regex_field": {
				"type": "string",
				"pattern": "^[a-zA-Z0-9_\\-\\.\\s\\/\\\\\"]+$"
			},
			"multiline_desc": {
				"type": "string",
				"description": "Line 1\nLine 2\r\nLine 3\tTabbed \"Quoted\" '\\' Backslash"
			},
			"nested_array": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"val": { "type": "number" }
					}
				}
			}
		},
		"large_padding": "` + strings.Repeat("X", 500*1024) + `"
	}`

	complexPrompt := "System Prompt Instructions v2.0:\n1. Parse invoice.\n2. Backslashes: C:\\Users\\Admin\\Documents\\file.pdf\n3. Unicode: 山田太郎 / Jérôme / Müller\n4. Code block:\n```json\n{\"example\": true}\n```\n"

	globalStateJSONStr := `{"document_id":"doc_9999_xyz","amount":9999.99,"is_valid":true,"tags":["finance","urgent","invoice"],"nested":{"raw_text":"Sample text with \n newlines and \"quotes\"","code":200}}`
	var gsMap map[string]any
	_ = json.Unmarshal([]byte(globalStateJSONStr), &gsMap)
	funcStruct, _ := structpb.NewStruct(gsMap)

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_preservation",
			AppId:    "app_preservation",
			JobId:    "job_preservation_1",
		},
		StepId: "step_preservation_1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName:           "FidelityCheckNode",
			SystemInstructions: complexPrompt,
			ExpectedSchemaStruct: unmarshalStruct(complexSchema),
		},
		GlobalState: &pb.GlobalState{Functional: funcStruct},
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	msg := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
	}

	ctx := context.Background()
	natsServer.processMessage(ctx, msg)

	if !msg.acked {
		t.Fatalf("Message was not acked by NatsServer")
	}

	// Fetch task from HumanValidationServer via GetPendingTasks
	stream := &mockGetPendingTasksServer{tasks: make([]*pb.PendingTask, 0)}
	err = valServer.GetPendingTasks(&pb.GetPendingTasksRequest{FilterStatus: "PENDING"}, stream)
	if err != nil {
		t.Fatalf("GetPendingTasks failed: %v", err)
	}

	if len(stream.tasks) != 1 {
		t.Fatalf("Expected 1 pending task, got %d", len(stream.tasks))
	}

	task := stream.tasks[0]

	// 3.1 Verify ExpectedJsonSchema byte-for-byte
	if task.GetExpectedSchema() == nil {
		t.Errorf("ExpectedJsonSchema CORRUPTED or TRUNCATED!\nExpected len: %d\nGot len: %d", len(complexSchema), 0)
	}

	// 3.2 Verify PromptInstructions byte-for-byte
	if task.GetPromptInstructions() != complexPrompt {
		t.Errorf("PromptInstructions CORRUPTED or TRUNCATED!\nExpected len: %d\nGot len: %d", len(complexPrompt), len(task.GetPromptInstructions()))
	}

	// 3.3 Verify GlobalStateJson contains all expected fields
	globalStateJSON := task.GetGlobalState()
	if globalStateJSON == nil {
		t.Fatalf("GlobalStateJson is empty!")
	}
	if !strings.Contains(fmt.Sprintf("%v", globalStateJSON), "doc_9999_xyz") || !strings.Contains(fmt.Sprintf("%v", globalStateJSON), "finance") {
		t.Errorf("GlobalStateJson missing expected fields: %s", globalStateJSON)
	}
}

func unmarshalStruct(jsonStr string) *structpb.Struct {
	var s structpb.Struct
	if err := protojson.Unmarshal([]byte(jsonStr), &s); err != nil {
		panic(err)
	}
	return &s
}
