package storage

import (
	"context"
	"errors"
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
	sessionID   string
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	tempFile    string
	done        chan struct{}
	sendChan    chan *compiler_service.ExecuteResponse
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	wg          sync.WaitGroup
	wrapperFile string
}

func NewClient(sessionID string, ctx context.Context, cancel context.CancelFunc) *Client {
	return &Client{
		sessionID: sessionID,
		done:      make(chan struct{}),
		sendChan:  make(chan *compiler_service.ExecuteResponse, 100),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (c *Client) SendResponse(resp *compiler_service.ExecuteResponse) {
	if c.ctx.Err() != nil {
		log.Printf("[%s] Context cancelled, dropping response: %v", c.sessionID, resp.Payload)
		return
	}
	select {
	case c.sendChan <- resp:
	default:
		log.Printf("[%s] Dropped response for session: channel full. Payload: %v", c.sessionID, resp.Payload)
	}
}

func (c *Client) SendResponses(stream compiler_service.CodeExecutor_ExecuteServer) {
	defer log.Printf("[%s] SendResponses goroutine finished.", c.sessionID)
	for {
		select {
		case <-c.ctx.Done():
			log.Printf("[%s] Context done in SendResponses: %v", c.sessionID, c.ctx.Err())
			return
		case resp, ok := <-c.sendChan:
			if !ok {
				log.Printf("[%s] Send channel closed.", c.sessionID)
				return
			}
			if err := stream.Send(resp); err != nil {
				log.Printf("[%s] Failed to send response: %v", c.sessionID, err)
				c.cancel()
				return
			}
		}
	}
}

func (c *Client) HandleInput(input string) {
	c.mu.Lock()
	stdin := c.stdin
	c.mu.Unlock()

	if stdin == nil {
		log.Printf("[%s] Received input %q but stdin is nil.", c.sessionID, input)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: "No active process to receive input"},
			},
		})
		return
	}

	log.Printf("[%s] Writing input to stdin: %q", c.sessionID, input)
	if _, err := fmt.Fprintf(stdin, "%s\n", input); err != nil {
		log.Printf("[%s] Error writing to stdin: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to write input: %v", err)},
			},
		})
	}
}

func (c *Client) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("[%s] Starting cleanup...", c.sessionID)
	c.cancel()

	if c.stdin != nil {
		log.Printf("[%s] Closing stdin pipe", c.sessionID)
		c.stdin.Close()
		c.stdin = nil
	}

	if c.cmd != nil && c.cmd.Process != nil {
		log.Printf("[%s] Killing process %d", c.sessionID, c.cmd.Process.Pid)
		if err := c.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("[%s] Failed to kill process: %v", c.sessionID, err)
		} else {
			log.Printf("[%s] Process killed or already done", c.sessionID)
		}
		c.cmd.Process.Release()
	}
	c.cmd = nil

	if c.tempFile != "" {
		log.Printf("[%s] Removing temp file: %s", c.sessionID, c.tempFile)
		os.Remove(c.tempFile)
		c.tempFile = ""
	}

	if c.wrapperFile != "" {
		log.Printf("[%s] Removing wrapper file: %s", c.sessionID, c.wrapperFile)
		os.Remove(c.wrapperFile)
		c.wrapperFile = ""
	}

	select {
	case <-c.done:
	default:
		close(c.done)
	}

	log.Printf("[%s] Cleanup finished.", c.sessionID)
}

func (c *Client) ReadOutputs(stdout, stderr io.Reader) {
	defer c.wg.Done()

	outputWg := sync.WaitGroup{}
	outputWg.Add(2)

	go func() {
		defer outputWg.Done()
		buf := make([]byte, 1024)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				outputChunk := string(buf[:n])
				log.Printf("[%s] STDOUT Raw Chunk (%d bytes): %q", c.sessionID, n, outputChunk)
				c.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: c.sessionID,
					Payload: &compiler_service.ExecuteResponse_Output{
						Output: &compiler_service.Output{OutputText: outputChunk},
					},
				})

				trimmedChunk := strings.TrimSpace(outputChunk)
				if strings.HasSuffix(trimmedChunk, ":") || strings.HasSuffix(trimmedChunk, "?") || strings.HasSuffix(trimmedChunk, ": ") || strings.HasSuffix(trimmedChunk, "? ") {
					log.Printf("[%s] Detected input prompt, sending WAITING_FOR_INPUT", c.sessionID)
					c.SendResponse(&compiler_service.ExecuteResponse{
						SessionId: c.sessionID,
						Payload: &compiler_service.ExecuteResponse_Status{
							Status: &compiler_service.Status{State: "WAITING_FOR_INPUT"},
						},
					})
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
					log.Printf("[%s] STDOUT closed/EOF.", c.sessionID)
				} else {
					log.Printf("[%s] Error reading stdout: %v", c.sessionID, err)
					c.SendResponse(&compiler_service.ExecuteResponse{
						SessionId: c.sessionID,
						Payload: &compiler_service.ExecuteResponse_Error{
							Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Stdout read error: %v", err)},
						},
					})
				}
				break
			}
		}
		log.Printf("[%s] STDOUT reader goroutine finished.", c.sessionID)
	}()

	go func() {
		defer outputWg.Done()
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				errorChunk := string(buf[:n])
				log.Printf("[%s] STDERR Raw Chunk (%d bytes): %q", c.sessionID, n, errorChunk)
				c.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: c.sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: errorChunk},
					},
				})
			}
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
					log.Printf("[%s] STDERR closed/EOF.", c.sessionID)
				} else {
					log.Printf("[%s] Error reading stderr: %v", c.sessionID, err)
					c.SendResponse(&compiler_service.ExecuteResponse{
						SessionId: c.sessionID,
						Payload: &compiler_service.ExecuteResponse_Error{
							Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Stderr read error: %v", err)},
						},
					})
				}
				break
			}
		}
		log.Printf("[%s] STDERR reader goroutine finished.", c.sessionID)
	}()

	outputWg.Wait()
	log.Printf("[%s] ReadOutputs completed (both readers finished).", c.sessionID)
}

func (c *Client) ExecutePython(code string) {
	defer c.Cleanup()
	defer func() {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}()

	log.Printf("[%s] Starting ExecutePython", c.sessionID)

	tempFile := fmt.Sprintf("/tmp/python-%s.py", uuid.New().String())
	c.mu.Lock()
	c.tempFile = tempFile
	c.mu.Unlock()

	log.Printf("[%s] Writing code to %s", c.sessionID, tempFile)
	if err := os.WriteFile(tempFile, []byte(code), 0644); err != nil {
		log.Printf("[%s] Failed to write temp file: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to write temp file: %v", err)},
			},
		})
		return
	}

	containerScriptPath := "/tmp/script.py"
	containerWrapperPath := "/tmp/wrapper.py"

	copyCmd := exec.CommandContext(c.ctx, "docker", "cp", tempFile, "online_compiler-python-runner-1:"+containerScriptPath)
	log.Printf("[%s] Copying %s to container:%s", c.sessionID, tempFile, containerScriptPath)
	if err := copyCmd.Run(); err != nil {
		log.Printf("[%s] Failed to copy code to container: %v", c.sessionID, err)
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
import os

original_print = builtins.print
def custom_print(*args, **kwargs):
    kwargs.setdefault('flush', True)
    original_print(*args, **kwargs)

builtins.print = custom_print

script_path = "/tmp/script.py"
try:
    with open(script_path) as f:
        script = f.read()
    compiled_code = compile(script, script_path, 'exec')
    exec(compiled_code, {'__name__': '__main__'})
except Exception as e:
    import traceback
    print(f"--- Python Execution Error ---", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
finally:
    try:
        os.remove(script_path)
        print(f"--- Cleaned up {script_path} ---", file=sys.stderr, flush=True)
    except OSError as e:
        print(f"--- Failed to cleanup {script_path}: {e} ---", file=sys.stderr, flush=True)
`
	wrapperFile := fmt.Sprintf("/tmp/python-wrapper-%s.py", uuid.New().String())
	log.Printf("[%s] Writing wrapper to %s", c.sessionID, wrapperFile)
	if err := os.WriteFile(wrapperFile, []byte(wrappedCode), 0644); err != nil {
		log.Printf("[%s] Failed to write wrapper file: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to write wrapper file: %v", err)},
			},
		})
		return
	}
	c.mu.Lock()
	c.wrapperFile = wrapperFile
	c.mu.Unlock()

	wrapperCopyCmd := exec.CommandContext(c.ctx, "docker", "cp", wrapperFile, "online_compiler-python-runner-1:"+containerWrapperPath)
	log.Printf("[%s] Copying %s to container:%s", c.sessionID, wrapperFile, containerWrapperPath)
	if err := wrapperCopyCmd.Run(); err != nil {
		log.Printf("[%s] Failed to copy wrapper to container: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to copy wrapper file to container: %v", err)},
			},
		})
		return
	}

	cmd := exec.CommandContext(c.ctx, "docker", "exec", "-i", "online_compiler-python-runner-1", "python3", "-u", containerWrapperPath)
	log.Printf("[%s] Preparing command: %s", c.sessionID, strings.Join(cmd.Args, " "))
	c.mu.Lock()
	c.cmd = cmd
	c.mu.Unlock()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Printf("[%s] Failed to create stdin pipe: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stdin pipe: %v", err)},
			},
		})
		return
	}
	c.mu.Lock()
	c.stdin = stdin
	c.mu.Unlock()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("[%s] Failed to create stdout pipe: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stdout pipe: %v", err)},
			},
		})
		stdin.Close()
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("[%s] Failed to create stderr pipe: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stderr pipe: %v", err)},
			},
		})
		stdin.Close()
		return
	}

	log.Printf("[%s] Starting command...", c.sessionID)
	if err := cmd.Start(); err != nil {
		log.Printf("[%s] Failed to start command: %v", c.sessionID, err)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to start command: %v", err)},
			},
		})
		stdin.Close()
		return
	}
	log.Printf("[%s] Command started with PID %d", c.sessionID, cmd.Process.Pid)

	c.wg.Add(1)
	go c.ReadOutputs(stdout, stderr)

	log.Printf("[%s] Waiting for command to complete...", c.sessionID)
	waitErr := cmd.Wait()
	log.Printf("[%s] Command finished (err: %v). Waiting for output processing...", c.sessionID, waitErr)
	c.wg.Wait()
	log.Printf("[%s] Output processing finished.", c.sessionID)

	if waitErr != nil {
		if errors.Is(waitErr, context.Canceled) {
			log.Printf("[%s] Execution cancelled by context: %v", c.sessionID, waitErr)
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Status{
					Status: &compiler_service.Status{State: "CANCELLED"},
				},
			})
		} else if errors.Is(waitErr, context.DeadlineExceeded) {
			log.Printf("[%s] Execution timed out: %v", c.sessionID, waitErr)
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: "Execution timed out after 30 seconds"},
				},
			})
		} else if exitErr, ok := waitErr.(*exec.ExitError); ok {
			log.Printf("[%s] Execution failed with exit code %d: %v", c.sessionID, exitErr.ExitCode(), waitErr)
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Python process exited with code %d", exitErr.ExitCode())},
				},
			})
		} else {
			log.Printf("[%s] Execution error: %v", c.sessionID, waitErr)
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Execution error: %v", waitErr)},
				},
			})
		}
	} else {
		log.Printf("[%s] Execution completed successfully.", c.sessionID)
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Status{
				Status: &compiler_service.Status{State: "EXECUTION_COMPLETE"},
			},
		})
	}

	close(c.sendChan)
	log.Printf("[%s] Send channel closed.", c.sessionID)

	log.Printf("[%s] ExecutePython finished.", c.sessionID)
}

func (c *Client) CtxDone() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}
