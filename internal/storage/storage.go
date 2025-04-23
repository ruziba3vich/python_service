package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
)

type Client struct {
	sessionID string
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	tempFile  string
	done      chan struct{}
	sendChan  chan *compiler_service.ExecuteResponse
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
}

func (c *Client) ReadOutputs(stdout, stderr io.Reader) {
	stdoutScanner := bufio.NewScanner(stdout)
	stderrScanner := bufio.NewScanner(stderr)
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})

	go func() {
		for stdoutScanner.Scan() {
			line := stdoutScanner.Text()
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Output{
					Output: &compiler_service.Output{OutputText: line},
				},
			})
			if strings.Contains(line, ":") || strings.Contains(line, "?") {
				c.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: c.sessionID,
					Payload: &compiler_service.ExecuteResponse_Status{
						Status: &compiler_service.Status{State: "WAITING_FOR_INPUT"},
					},
				})
			}
		}
		close(stdoutDone)
	}()

	go func() {
		for stderrScanner.Scan() {
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: stderrScanner.Text()},
				},
			})
		}
		close(stderrDone)
	}()

	<-stdoutDone
	<-stderrDone
}

func (c *Client) SendResponse(resp *compiler_service.ExecuteResponse) {
	select {
	case c.sendChan <- resp:
	default:
		log.Printf("Dropped response for session %s: channel full", c.sessionID)
	}
}

func (c *Client) SendResponses(stream compiler_service.CodeExecutor_ExecuteServer) {
	for {
		select {
		case <-c.ctx.Done():
			return
		case resp := <-c.sendChan:
			if err := stream.Send(resp); err != nil {
				log.Printf("Failed to send response for session %s: %v", c.sessionID, err)
				return
			}
		}
	}
}

func (c *Client) HandleInput(input string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stdin != nil {
		fmt.Fprintf(c.stdin, "%s\n", input)
	}
}

func (c *Client) Cleanup() {
	if c.tempFile != "" {
		os.Remove(c.tempFile)
	}
	if c.cmd != nil && c.cmd.Process != nil {
		c.cmd.Process.Kill()
	}
	c.cancel()
}

func (c *Client) ExecutePython(code string) {
	defer c.Cleanup()
	tempFile := fmt.Sprintf("/tmp/python-%s.py", uuid.New().String())
	c.tempFile = tempFile
	if err := os.WriteFile(tempFile, []byte(code), 0644); err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to write temp file: %v", err)},
			},
		})
		return
	}

	copyCmd := exec.Command("docker", "cp", tempFile, "online_compiler-python-runner-1:/tmp/script.py")
	if err := copyCmd.Run(); err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to copy file to container: %v", err)},
			},
		})
		return
	}

	wrappedCode := `
import builtins
import sys

original_print = builtins.print
def custom_print(*args, **kwargs):
    if 'flush' not in kwargs:
        kwargs['flush'] = True
    original_print(*args, **kwargs)

builtins.print = custom_print

with open("/tmp/script.py") as f:
    script = f.read()
    exec(script)
`

	wrapperFile := fmt.Sprintf("/tmp/python-wrapper-%s.py", uuid.New().String())
	if err := os.WriteFile(wrapperFile, []byte(wrappedCode), 0644); err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to write wrapper file: %v", err)},
			},
		})
		return
	}
	defer os.Remove(wrapperFile)

	wrapperCopyCmd := exec.Command("docker", "cp", wrapperFile, "online_compiler-python-runner-1:/tmp/wrapper.py")
	if err := wrapperCopyCmd.Run(); err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to copy wrapper file to container: %v", err)},
			},
		})
		return
	}

	cmd := exec.CommandContext(c.ctx, "docker", "exec", "-i", "online_compiler-python-runner-1", "python3", "-u", "/tmp/wrapper.py")
	c.cmd = cmd

	stdin, err := cmd.StdinPipe()
	if err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stdin pipe: %v", err)},
			},
		})
		return
	}
	c.stdin = stdin

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stdout pipe: %v", err)},
			},
		})
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stderr pipe: %v", err)},
			},
		})
		return
	}

	if err := cmd.Start(); err != nil {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to start command: %v", err)},
			},
		})
		return
	}

	outputDone := make(chan struct{})
	go func() {
		c.ReadOutputs(stdout, stderr)
		close(outputDone)
	}()

	err = cmd.Wait()
	close(c.done)

	<-outputDone

	if err != nil && c.ctx.Err() != context.DeadlineExceeded {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Execution error: %v", err)},
			},
		})
	} else {
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Status{
				Status: &compiler_service.Status{State: "EXECUTION_COMPLETE"},
			},
		})
	}
}
