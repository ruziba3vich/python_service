package service

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
	"github.com/ruziba3vich/python_service/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
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

func (s *PythonExecutorServer) removeClient(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if client, exists := s.clients[sessionID]; exists {
		log.Printf("[%s] Removing client session.", sessionID)
		client.Cleanup()
		delete(s.clients, sessionID)
		log.Printf("[%s] Client session removed.", sessionID)
	} else {
		log.Printf("[%s] Client session already removed.", sessionID)
	}
}

func (s *PythonExecutorServer) Execute(stream compiler_service.CodeExecutor_ExecuteServer) error {
	sessionID := ""
	var client *storage.Client
	var clientAddr string

	if p, ok := peer.FromContext(stream.Context()); ok {
		clientAddr = p.Addr.String()
	}
	log.Printf("New Execute stream started from %s", clientAddr)

	defer func() {
		if sessionID != "" {
			log.Printf("[%s] Execute stream ended. Initiating cleanup for session.", sessionID)
			s.removeClient(sessionID)
		} else {
			log.Printf("Execute stream ended before session was established from %s", clientAddr)
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Printf("[%s] Stream closed by client (EOF).", sessionID)
				return nil
			}
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
				log.Printf("[%s] Stream context cancelled or deadline exceeded: %v", sessionID, err)
				return err
			}
			log.Printf("[%s] Stream receive error: %v", sessionID, err)
			return status.Errorf(codes.Internal, "stream error: %v", err)
		}

		if client == nil {
			sessionID = req.SessionId
			if sessionID == "" {
				sessionID = uuid.NewString()
				log.Printf("No session_id provided by %s, generated new one: %s", clientAddr, sessionID)
			}
			log.Printf("[%s] Initial request received.", sessionID)

			s.mu.Lock()
			if existingClient, exists := s.clients[sessionID]; exists {
				s.mu.Unlock()
				if existingClient.CtxDone() {
					log.Printf("[%s] Stale session found, cleaning up and allowing new one.", sessionID)
					s.removeClient(sessionID)
					s.mu.Lock()
				} else {
					log.Printf("[%s] Session already exists and is active.", sessionID)
					return status.Errorf(codes.AlreadyExists, "session %s already exists", sessionID)
				}
			}
			clientCtx, clientCancel := context.WithTimeout(context.Background(), 60*time.Second)
			linkedCtx, linkedCancel := context.WithCancel(stream.Context())

			go func() {
				select {
				case <-linkedCtx.Done():
					log.Printf("[%s] Stream context done (%v), cancelling client context.", sessionID, linkedCtx.Err())
					clientCancel()
				case <-clientCtx.Done():
					linkedCancel()
				}
			}()

			client = storage.NewClient(sessionID, clientCtx, clientCancel)
			s.clients[sessionID] = client
			log.Printf("[%s] New client created.", sessionID)
			s.mu.Unlock()

			go client.SendResponses(stream)

		} else {
			if req.SessionId != sessionID {
				log.Printf("[%s] Received message with mismatched session ID %s", sessionID, req.SessionId)
				client.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Mismatched session ID: expected %s, got %s", sessionID, req.SessionId)},
					},
				})
				continue
			}
		}

		switch payload := req.Payload.(type) {
		case *compiler_service.ExecuteRequest_Code:
			log.Printf("[%s] Received Code payload", sessionID)
			if payload.Code.Language != "python" {
				log.Printf("[%s] Unsupported language: %s", sessionID, payload.Code.Language)
				client.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: "Only Python is supported"},
					},
				})
				continue
			}
			log.Printf("[%s] Starting Python execution...", sessionID)
			go client.ExecutePython(payload.Code.SourceCode)

		case *compiler_service.ExecuteRequest_Input:
			log.Printf("[%s] Received Input payload: %q", sessionID, payload.Input.InputText)
			client.HandleInput(payload.Input.InputText)

		default:
			log.Printf("[%s] Received unknown payload type: %T", sessionID, payload)
			client.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: "Unknown request payload type"},
				},
			})
		}
	}
}
