package service

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
	"github.com/ruziba3vich/python_service/internal/storage"
	"github.com/ruziba3vich/python_service/pkg/lgg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type PythonExecutorServer struct {
	compiler_service.UnimplementedCodeExecutorServer
	clients map[string]*storage.Client
	mu      sync.Mutex
	logger  *lgg.Logger
}

func NewPythonExecutorServer() (*PythonExecutorServer, error) {
	logger, err := lgg.NewLogger()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &PythonExecutorServer{
		clients: make(map[string]*storage.Client),
		logger:  logger,
	}, nil
}

func (s *PythonExecutorServer) removeClient(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client, exists := s.clients[sessionID]; exists {
		s.logger.Info("Removing client session", map[string]any{"session_id": sessionID})
		client.Cleanup()
		delete(s.clients, sessionID)
		s.logger.Info("Client session removed", map[string]any{"session_id": sessionID})
	} else {
		s.logger.Warn("Client session already removed", map[string]any{"session_id": sessionID})
	}
}

func (s *PythonExecutorServer) Execute(stream compiler_service.CodeExecutor_ExecuteServer) error {
	sessionID := ""
	var client *storage.Client
	var clientAddr string

	if p, ok := peer.FromContext(stream.Context()); ok {
		clientAddr = p.Addr.String()
	}
	s.logger.Info("New Execute stream started", map[string]any{"client_addr": clientAddr})

	defer func() {
		if sessionID != "" {
			s.logger.Info("Execute stream ended. Initiating cleanup for session",
				map[string]any{"session_id": sessionID})
			s.removeClient(sessionID)
		} else {
			s.logger.Info("Execute stream ended before session was established",
				map[string]any{"client_addr": clientAddr})
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.logger.Info("Stream closed by client (EOF)", map[string]any{"session_id": sessionID})
				return nil
			}
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
				s.logger.Warn("Stream context cancelled or deadline exceeded",
					map[string]any{"session_id": sessionID, "error": err})
				return err
			}
			s.logger.Error(fmt.Sprintf("Stream receive error: %v", err),
				map[string]any{"session_id": sessionID, "error": err})
			return status.Errorf(codes.Internal, "stream error: %v", err)
		}

		if client == nil {
			sessionID = req.SessionId
			if sessionID == "" {
				sessionID = uuid.NewString()
				s.logger.Warn("No session_id provided, generated new one",
					map[string]any{"client_addr": clientAddr, "session_id": sessionID})
			}
			s.logger.Info("Initial request received", map[string]any{"session_id": sessionID})

			s.mu.Lock()
			if existingClient, exists := s.clients[sessionID]; exists {
				s.mu.Unlock()
				if existingClient.CtxDone() {
					s.logger.Info("Stale session found, cleaning up and allowing new one",
						map[string]any{"session_id": sessionID})
					s.removeClient(sessionID)
					s.mu.Lock()
				} else {
					s.logger.Error("Session already exists and is active",
						map[string]any{"session_id": sessionID})
					return status.Errorf(codes.AlreadyExists, "session %s already exists", sessionID)
				}
			}
			clientCtx, clientCancel := context.WithTimeout(context.Background(), 60*time.Second)
			linkedCtx, linkedCancel := context.WithCancel(stream.Context())

			go func() {
				select {
				case <-linkedCtx.Done():
					s.logger.Info(fmt.Sprintf("Stream context done (%v), cancelling client context",
						linkedCtx.Err()), map[string]any{"session_id": sessionID})
					clientCancel()
				case <-clientCtx.Done():
					linkedCancel()
				}
			}()

			client = storage.NewClient(sessionID, clientCtx, clientCancel, s.logger)
			s.clients[sessionID] = client
			s.logger.Info("New client created", map[string]any{"session_id": sessionID})
			s.mu.Unlock()

			go client.SendResponses(stream)

		} else {
			if req.SessionId != sessionID {
				s.logger.Warn(fmt.Sprintf("Received message with mismatched session ID %s", req.SessionId),
					map[string]any{"session_id": sessionID, "received_session_id": req.SessionId})
				client.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{
							ErrorText: fmt.Sprintf("Mismatched session ID: expected %s, got %s", sessionID, req.SessionId),
						},
					},
				})
				continue
			}
		}

		switch payload := req.Payload.(type) {
		case *compiler_service.ExecuteRequest_Code:
			s.logger.Info("Received Code payload", map[string]any{"session_id": sessionID})
			if payload.Code.Language != "python" {
				s.logger.Warn(fmt.Sprintf("Unsupported language: %s", payload.Code.Language),
					map[string]any{"session_id": sessionID, "language": payload.Code.Language})
				client.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: "Only Python is supported"},
					},
				})
				continue
			}
			s.logger.Info("Starting Python execution", map[string]any{"session_id": sessionID})
			go client.ExecutePython(payload.Code.SourceCode)

		case *compiler_service.ExecuteRequest_Input:
			s.logger.Info(fmt.Sprintf("Received Input payload: %q", payload.Input.InputText),
				map[string]any{"session_id": sessionID, "input": payload.Input.InputText})
			client.HandleInput(payload.Input.InputText)

		default:
			s.logger.Warn(fmt.Sprintf("Received unknown payload type: %T", payload),
				map[string]any{"session_id": sessionID, "payload_type": fmt.Sprintf("%T", payload)})
			client.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: "Unknown request payload type"},
				},
			})
		}
	}
}
