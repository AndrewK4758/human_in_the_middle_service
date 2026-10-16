package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	os.Setenv("WORKERS_STREAM", "WORKER_TASKS")
	os.Setenv("ORCHESTRATOR_ADDRESS", "localhost:11112")
}

func TestChallenger_SubmitCorrection_EdgeCases(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()
	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	s := NewHumanValidationServer(&mockJetStreamForServer{}, pb.NewOrchestratorServiceClient(conn))
	ctx := context.Background()

	_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    "non_existent_123",
		Corrected: unmarshalStruct(`{"key": "value"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected 'not found' error for non-existent task, got: %v", err)
	}

	_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    "",
		Corrected: unmarshalStruct(`{"key": "value"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "missing required taskId") {
		t.Fatalf("Expected 'missing required taskId' error for empty task ID, got: %v", err)
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Expected panic for CreateValidationTask with nil identity, got none")
			}
		}()
		_, _ = s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
			Identity: nil,
			StepId:   "step_nil_id",
		})
	}()

	createResp2, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_status",
			AppId:    "app_status",
			JobId:    "job_status_test",
		},
		StepId: "step_status",
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
		setTaskStatus(s, taskIdStatus, invalidStatus)

		_, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
			TaskId:    taskIdStatus,
			Corrected: unmarshalStruct(`{"fixed": true}`),
		})
		if err == nil {
			t.Errorf("Expected error when task is in status %s, but got none", invalidStatus)
		} else if !strings.Contains(err.Error(), "already") {
			t.Errorf("Expected error message containing 'already', got: %v", err)
		}
	}

	setTaskStatus(s, taskIdStatus, TaskStatusPending)

	largeJson2MB := `{"large_field": "` + strings.Repeat("A", 2*1024*1024) + `", "unicode": "こんにちは世界 🚀 \n\t\""}`
	var receivedJson string
	mockOrch.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
		b, _ := protojson.Marshal(req.GetCorrected())
		receivedJson = string(b)
		return &pb.ResumeJobResponse{Success: true, Message: "large json received"}, nil
	}

	respLarge, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    taskIdStatus,
		Corrected: unmarshalStruct(largeJson2MB),
	})
	if err != nil {
		t.Fatalf("SubmitCorrection failed with 2MB payload: %v", err)
	}
	if !respLarge.GetSuccess() {
		t.Fatalf("Expected success for 2MB payload")
	}
	// Compare lengths rather than exact strings since protojson formatting may vary.
	if len(receivedJson) < len(largeJson2MB)-100 {
		t.Errorf("Payload byte corruption detected for 2MB JSON!\nExpected len: %d\nGot len: %d", len(largeJson2MB), len(receivedJson))
	}

	// Payloads over 4MB exceed the default gRPC client limit and should roll the task back to PENDING.
	createRespExceed, _ := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_exceed",
			AppId:    "app_exceed",
			JobId:    "job_exceed_4mb",
		},
		StepId: "step_exceed",
	})
	taskIdExceed := createRespExceed.GetTaskId()

	payload5MB := `{"large_field": "` + strings.Repeat("B", 5*1024*1024) + `"}`
	_, errExceed := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    taskIdExceed,
		Corrected: unmarshalStruct(payload5MB),
	})
	if errExceed == nil {
		t.Fatalf("Expected error when sending 5MB message over gRPC default client")
	}
	if !strings.Contains(errExceed.Error(), "ResourceExhausted") && !strings.Contains(errExceed.Error(), "larger than max") {
		t.Logf("gRPC error: %v", errExceed)
	}

	statusExceed := getTask(s, taskIdExceed).Status
	if statusExceed != TaskStatusPending {
		t.Fatalf("Expected status rolled back to PENDING when >4MB payload fails gRPC limit, got: %s", statusExceed)
	}
}

func TestChallenger_NatsServer_ProcessMessage_EdgeCases(t *testing.T) {
	valServer := NewHumanValidationServer(&mockJetStreamForServer{}, nil)
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}
	ctx := context.Background()

	msgEmpty := &mockMsg{
		data:    []byte{},
		subject: "worker.human.validation",
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for empty byte slice (missing identity)")
			}
		}()
		natsServer.processMessage(ctx, msgEmpty)
	}()

	reqNilFields := &pb.ExecuteTaskRequest{}
	reqBytes, _ := proto.Marshal(reqNilFields)
	msgNilFields := &mockMsg{
		data:    reqBytes,
		subject: "worker.human.validation",
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for request with nil identity fields")
			}
		}()
		natsServer.processMessage(ctx, msgNilFields)
	}()

	mockJSFail := newMockJetStream()
	mockJSFail.publishErr = fmt.Errorf("JetStream storage full")
	natsServerFail := &NatsServer{
		nc:               nil,
		js:               mockJSFail,
		validationServer: valServer,
	}

	headers := nats.Header{}
	headers.Set("Reply-Subject", "orchestrator.reply.fail")
	validReq := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t", AppId: "a", JobId: "j"},
		TaskSubject: "worker.human.validation",
		Payload: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"node_config": structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"node_name":              structpb.NewStringValue("TestNode"),
						"system_instructions":    structpb.NewStringValue("instructions"),
						"expected_schema_struct": structpb.NewStructValue(&structpb.Struct{}),
					},
				}),
				"global_state": structpb.NewStructValue(&structpb.Struct{
					Fields: map[string]*structpb.Value{
						"functional": structpb.NewStructValue(&structpb.Struct{}),
					},
				}),
			},
		},
	}
	validReqBytes, _ := proto.Marshal(validReq)

	msgReplyFail := &mockMsg{
		data:    validReqBytes,
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

func TestChallenger_OrchestratorDisconnectAndRollback(t *testing.T) {
	mockOrch := &mockOrchestratorServer{}
	addr, cleanup := startMockOrchestrator(t, mockOrch)
	defer cleanup()
	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	s := NewHumanValidationServer(&mockJetStreamForServer{}, pb.NewOrchestratorServiceClient(conn))
	ctx := context.Background()

	createResp, err := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_disc",
			AppId:    "app_disc",
			JobId:    "job_disconnect_test",
		},
		StepId: "step_disc",
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	taskId := createResp.GetTaskId()

	// Stop the orchestrator server to simulate a hard network disconnect.
	cleanup()

	_, err = s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    taskId,
		Corrected: unmarshalStruct(`{"data": 1}`),
	})
	if err == nil {
		t.Fatalf("Expected error when orchestrator is completely shut down")
	}

	statusAfterCrash := getTask(s, taskId).Status

	if statusAfterCrash != TaskStatusPending {
		t.Fatalf("Expected task status rolled back to PENDING after server crash, got: %s", statusAfterCrash)
	}

	mockOrch2 := &mockOrchestratorServer{}
	newAddr, cleanup2 := startMockOrchestrator(t, mockOrch2)
	defer cleanup2()
	conn2, _ := grpc.NewClient(newAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn2.Close()
	s.orchClient = pb.NewOrchestratorServiceClient(conn2)
	resp, err := s.SubmitCorrection(ctx, &pb.SubmitCorrectionRequest{
		TaskId:    taskId,
		Corrected: unmarshalStruct(`{"data": 1}`),
	})
	if err != nil {
		t.Fatalf("SubmitCorrection failed after orchestrator recovery: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Expected success after orchestrator recovery")
	}

	finalStatus := getTask(s, taskId).Status
	if finalStatus != TaskStatusResolved {
		t.Fatalf("Expected final status RESOLVED after recovery, got: %s", finalStatus)
	}

	createResp3, _ := s.CreateValidationTask(ctx, &pb.CreateTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_timeout",
			AppId:    "app_timeout",
			JobId:    "job_ctx_timeout",
		},
		StepId: "step_timeout",
	})
	taskIdTimeout := createResp3.GetTaskId()

	mockOrch2.resumeJobFunc = func(ctx context.Context, req *pb.ResumeJobRequest, opts ...grpc.CallOption) (*pb.ResumeJobResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err = s.SubmitCorrection(shortCtx, &pb.SubmitCorrectionRequest{
		TaskId:    taskIdTimeout,
		Corrected: unmarshalStruct(`{"data": 2}`),
	})
	if err == nil {
		t.Fatalf("Expected error when context times out during ResumeJob")
	}

	statusAfterTimeout := getTask(s, taskIdTimeout).Status

	if statusAfterTimeout != TaskStatusPending {
		t.Fatalf("Expected status rolled back to PENDING after context timeout, got: %s", statusAfterTimeout)
	}
}

func TestChallenger_StringPreservationFidelity(t *testing.T) {
	valServer := NewHumanValidationServer(&mockJetStreamForServer{}, nil)
	mockJS := newMockJetStream()

	natsServer := &NatsServer{
		nc:               nil,
		js:               mockJS,
		validationServer: valServer,
	}

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

	var complexSchemaMap map[string]interface{}
	_ = json.Unmarshal([]byte(complexSchema), &complexSchemaMap)

	payloadMap := map[string]interface{}{
		"node_config": map[string]interface{}{
			"node_name":              "FidelityCheckNode",
			"expected_schema_struct": complexSchemaMap,
			"system_instructions":    complexPrompt,
		},
		"global_state": map[string]interface{}{
			"functional": gsMap,
		},
	}
	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		t.Fatalf("Failed to create payload struct: %v", err)
	}

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{
			TenantId: "tenant_preservation",
			AppId:    "app_preservation",
			JobId:    "job_preservation_1",
		},
		TaskSubject: "worker.human.validation",
		StepId:      "step_preservation_1",
		Payload:     payloadStruct,
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

	stream := &mockGetPendingTasksServer{tasks: make([]*pb.PendingTask, 0)}
	err = valServer.GetPendingTasks(&pb.GetPendingTasksRequest{FilterStatus: "PENDING"}, stream)
	if err != nil {
		t.Fatalf("GetPendingTasks failed: %v", err)
	}

	if len(stream.tasks) != 1 {
		t.Fatalf("Expected 1 pending task, got %d", len(stream.tasks))
	}

	task := stream.tasks[0]

	if task.GetExpectedSchema() == nil {
		t.Errorf("ExpectedJsonSchema CORRUPTED or TRUNCATED!\nExpected len: %d\nGot len: %d", len(complexSchema), 0)
	}

	if task.GetPromptInstructions() != complexPrompt {
		t.Errorf("PromptInstructions CORRUPTED or TRUNCATED!\nExpected len: %d\nGot len: %d", len(complexPrompt), len(task.GetPromptInstructions()))
	}

	globalStateJSON := task.GetGlobalState()
	if globalStateJSON == nil {
		t.Fatalf("GlobalStateJson is empty!")
	}
	if !strings.Contains(fmt.Sprintf("%v", globalStateJSON), "doc_9999_xyz") || !strings.Contains(fmt.Sprintf("%v", globalStateJSON), "finance") {
		t.Errorf("GlobalStateJson missing expected fields: %s", globalStateJSON)
	}
}
