package service

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
	"github.com/ruziba3vich/python_service/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewPythonExecutorServer() *PythonExecutorServer {
	return &PythonExecutorServer{
		clients: make(map[string]*storage.Client),
	}
}

type PythonExecutorServer struct {
	compiler_service.UnimplementedCodeExecutorServer
	clients map[string]*storage.Client
	mu      sync.Mutex
}

func (s *PythonExecutorServer) Execute(stream compiler_service.CodeExecutor_ExecuteServer) error {
	sessionID := ""
	var client *storage.Client

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if client != nil {
				client.Cleanup()
			}
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "stream error: %v", err)
		}

		if sessionID == "" {
			sessionID = req.SessionId
			if sessionID == "" {
				return status.Error(codes.InvalidArgument, "session_id is required")
			}

			s.mu.Lock()
			if _, exists := s.clients[sessionID]; exists {
				s.mu.Unlock()
				return status.Errorf(codes.AlreadyExists, "session %s already exists", sessionID)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			client = storage.NewClient(sessionID, ctx, cancel)
			s.clients[sessionID] = client
			s.mu.Unlock()

			go client.SendResponses(stream)
		}

		switch payload := req.Payload.(type) {
		case *compiler_service.ExecuteRequest_Code:
			if payload.Code.Language != "python" {
				client.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: "Only Python is supported"},
					},
				})
				continue
			}
			go client.ExecutePython(payload.Code.SourceCode)

		case *compiler_service.ExecuteRequest_Input:
			client.HandleInput(payload.Input.InputText)
		}
	}
}
