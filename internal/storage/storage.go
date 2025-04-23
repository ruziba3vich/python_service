package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
	"github.com/ruziba3vich/python_service/pkg/lgg"
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
	logger      *lgg.Logger
}

func NewClient(sessionID string, ctx context.Context, cancel context.CancelFunc, logger *lgg.Logger) *Client {
	return &Client{
		sessionID: sessionID,
		done:      make(chan struct{}),
		sendChan:  make(chan *compiler_service.ExecuteResponse, 100),
		ctx:       ctx,
		cancel:    cancel,
		logger:    logger,
	}
}

func (c *Client) SendResponse(resp *compiler_service.ExecuteResponse) {
	if c.ctx.Err() != nil {
		c.logger.Warn(fmt.Sprintf("Context cancelled, dropping response: %v", resp.Payload),
			map[string]any{"session_id": c.sessionID, "payload": fmt.Sprintf("%v", resp.Payload)})
		return
	}
	select {
	case c.sendChan <- resp:
	default:
		c.logger.Warn("Dropped response: channel full",
			map[string]any{"session_id": c.sessionID, "payload": fmt.Sprintf("%v", resp.Payload)})
	}
}

func (c *Client) SendResponses(stream compiler_service.CodeExecutor_ExecuteServer) {
	defer c.logger.Info("SendResponses goroutine finished", map[string]any{"session_id": c.sessionID})
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info(fmt.Sprintf("Context done in SendResponses: %v", c.ctx.Err()),
				map[string]any{"session_id": c.sessionID})
			return
		case resp, ok := <-c.sendChan:
			if !ok {
				c.logger.Info("Send channel closed", map[string]any{"session_id": c.sessionID})
				return
			}
			if err := stream.Send(resp); err != nil {
				c.logger.Error(fmt.Sprintf("Failed to send response: %v", err),
					map[string]any{"session_id": c.sessionID, "error": err})
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
		c.logger.Error(fmt.Sprintf("Received input %q but stdin is nil", input),
			map[string]any{"session_id": c.sessionID, "input": input})
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: "No active process to receive input"},
			},
		})
		return
	}

	c.logger.Info(fmt.Sprintf("Writing input to stdin: %q", input),
		map[string]any{"session_id": c.sessionID, "input": input})
	if _, err := fmt.Fprintf(stdin, "%s\n", input); err != nil {
		c.logger.Error(fmt.Sprintf("Error writing to stdin: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
	c.logger.Info("Starting cleanup", map[string]any{"session_id": c.sessionID})
	c.cancel()

	if c.stdin != nil {
		c.logger.Info("Closing stdin pipe", map[string]any{"session_id": c.sessionID})
		c.stdin.Close()
		c.stdin = nil
	}

	if c.cmd != nil && c.cmd.Process != nil {
		c.logger.Info(fmt.Sprintf("Killing process %d", c.cmd.Process.Pid),
			map[string]any{"session_id": c.sessionID, "pid": c.cmd.Process.Pid})
		if err := c.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			c.logger.Error(fmt.Sprintf("Failed to kill process: %v", err),
				map[string]any{"session_id": c.sessionID, "error": err})
		} else {
			c.logger.Info("Process killed or already done", map[string]any{"session_id": c.sessionID})
		}
		c.cmd.Process.Release()
	}
	c.cmd = nil

	if c.tempFile != "" {
		c.logger.Info(fmt.Sprintf("Removing temp file: %s", c.tempFile),
			map[string]any{"session_id": c.sessionID, "file": c.tempFile})
		os.Remove(c.tempFile)
		c.tempFile = ""
	}

	if c.wrapperFile != "" {
		c.logger.Info(fmt.Sprintf("Removing wrapper file: %s", c.wrapperFile),
			map[string]any{"session_id": c.sessionID, "file": c.wrapperFile})
		os.Remove(c.wrapperFile)
		c.wrapperFile = ""
	}

	select {
	case <-c.done:
	default:
		close(c.done)
	}

	c.logger.Info("Cleanup finished", map[string]any{"session_id": c.sessionID})
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
				c.logger.Debug(fmt.Sprintf("STDOUT Raw Chunk (%d bytes): %q", n, outputChunk),
					map[string]any{"session_id": c.sessionID, "bytes": n, "chunk": outputChunk})
				c.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: c.sessionID,
					Payload: &compiler_service.ExecuteResponse_Output{
						Output: &compiler_service.Output{OutputText: outputChunk},
					},
				})

				trimmedChunk := strings.TrimSpace(outputChunk)
				if strings.HasSuffix(trimmedChunk, ":") || strings.HasSuffix(trimmedChunk, "?") || strings.HasSuffix(trimmedChunk, ": ") || strings.HasSuffix(trimmedChunk, "? ") {
					c.logger.Info("Detected input prompt, sending WAITING_FOR_INPUT",
						map[string]any{"session_id": c.sessionID, "chunk": trimmedChunk})
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
					c.logger.Info("STDOUT closed/EOF", map[string]any{"session_id": c.sessionID})
				} else {
					c.logger.Error(fmt.Sprintf("Error reading stdout: %v", err),
						map[string]any{"session_id": c.sessionID, "error": err})
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
		c.logger.Info("STDOUT reader goroutine finished", map[string]any{"session_id": c.sessionID})
	}()

	go func() {
		defer outputWg.Done()
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				errorChunk := string(buf[:n])
				c.logger.Debug(fmt.Sprintf("STDERR Raw Chunk (%d bytes): %q", n, errorChunk),
					map[string]any{"session_id": c.sessionID, "bytes": n, "chunk": errorChunk})
				c.SendResponse(&compiler_service.ExecuteResponse{
					SessionId: c.sessionID,
					Payload: &compiler_service.ExecuteResponse_Error{
						Error: &compiler_service.Error{ErrorText: errorChunk},
					},
				})
			}
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
					c.logger.Info("STDERR closed/EOF", map[string]any{"session_id": c.sessionID})
				} else {
					c.logger.Error(fmt.Sprintf("Error reading stderr: %v", err),
						map[string]any{"session_id": c.sessionID, "error": err})
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
		c.logger.Info("STDERR reader goroutine finished", map[string]any{"session_id": c.sessionID})
	}()

	outputWg.Wait()
	c.logger.Info("ReadOutputs completed (both readers finished)", map[string]any{"session_id": c.sessionID})
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

	c.logger.Info("Starting ExecutePython", map[string]any{"session_id": c.sessionID})

	tempFile := fmt.Sprintf("/tmp/python-%s.py", uuid.New().String())
	c.mu.Lock()
	c.tempFile = tempFile
	c.mu.Unlock()

	c.logger.Info(fmt.Sprintf("Writing code to %s", tempFile),
		map[string]any{"session_id": c.sessionID, "file": tempFile})
	if err := os.WriteFile(tempFile, []byte(code), 0644); err != nil {
		c.logger.Error(fmt.Sprintf("Failed to write temp file: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
	c.logger.Info(fmt.Sprintf("Copying %s to container:%s", tempFile, containerScriptPath),
		map[string]any{"session_id": c.sessionID, "source": tempFile, "destination": containerScriptPath})
	if err := copyCmd.Run(); err != nil {
		c.logger.Error(fmt.Sprintf("Failed to copy code to container: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
	c.logger.Info(fmt.Sprintf("Writing wrapper to %s", wrapperFile),
		map[string]any{"session_id": c.sessionID, "file": wrapperFile})
	if err := os.WriteFile(wrapperFile, []byte(wrappedCode), 0644); err != nil {
		c.logger.Error(fmt.Sprintf("Failed to write wrapper file: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
	c.logger.Info(fmt.Sprintf("Copying %s to container:%s", wrapperFile, containerWrapperPath),
		map[string]any{"session_id": c.sessionID, "source": wrapperFile, "destination": containerWrapperPath})
	if err := wrapperCopyCmd.Run(); err != nil {
		c.logger.Error(fmt.Sprintf("Failed to copy wrapper to container: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to copy wrapper file to container: %v", err)},
			},
		})
		return
	}

	cmd := exec.CommandContext(c.ctx, "docker", "exec", "-i", "online_compiler-python-runner-1", "python3", "-u", containerWrapperPath)
	c.logger.Info(fmt.Sprintf("Preparing command: %s", strings.Join(cmd.Args, " ")),
		map[string]any{"session_id": c.sessionID, "command": strings.Join(cmd.Args, " ")})
	c.mu.Lock()
	c.cmd = cmd
	c.mu.Unlock()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		c.logger.Error(fmt.Sprintf("Failed to create stdin pipe: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
		c.logger.Error(fmt.Sprintf("Failed to create stdout pipe: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
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
		c.logger.Error(fmt.Sprintf("Failed to create stderr pipe: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to create stderr pipe: %v", err)},
			},
		})
		stdin.Close()
		return
	}

	c.logger.Info("Starting command", map[string]any{"session_id": c.sessionID})
	if err := cmd.Start(); err != nil {
		c.logger.Error(fmt.Sprintf("Failed to start command: %v", err),
			map[string]any{"session_id": c.sessionID, "error": err})
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Error{
				Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Failed to start command: %v", err)},
			},
		})
		stdin.Close()
		return
	}
	c.logger.Info(fmt.Sprintf("Command started with PID %d", cmd.Process.Pid),
		map[string]any{"session_id": c.sessionID, "pid": cmd.Process.Pid})

	c.wg.Add(1)
	go c.ReadOutputs(stdout, stderr)

	c.logger.Info("Waiting for command to complete", map[string]any{"session_id": c.sessionID})
	waitErr := cmd.Wait()
	c.logger.Info(fmt.Sprintf("Command finished (err: %v). Waiting for output processing", waitErr),
		map[string]any{"session_id": c.sessionID, "error": waitErr})
	c.wg.Wait()
	c.logger.Info("Output processing finished", map[string]any{"session_id": c.sessionID})

	if waitErr != nil {
		if errors.Is(waitErr, context.Canceled) {
			c.logger.Info(fmt.Sprintf("Execution cancelled by context: %v", waitErr),
				map[string]any{"session_id": c.sessionID, "error": waitErr})
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Status{
					Status: &compiler_service.Status{State: "CANCELLED"},
				},
			})
		} else if errors.Is(waitErr, context.DeadlineExceeded) {
			c.logger.Error(fmt.Sprintf("Execution timed out: %v", waitErr),
				map[string]any{"session_id": c.sessionID, "error": waitErr})
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: "Execution timed out after 30 seconds"},
				},
			})
		} else if exitErr, ok := waitErr.(*exec.ExitError); ok {
			c.logger.Error(fmt.Sprintf("Execution failed with exit code %d: %v", exitErr.ExitCode(), waitErr),
				map[string]any{"session_id": c.sessionID, "exit_code": exitErr.ExitCode(), "error": waitErr})
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Python process exited with code %d", exitErr.ExitCode())},
				},
			})
		} else {
			c.logger.Error(fmt.Sprintf("Execution error: %v", waitErr),
				map[string]any{"session_id": c.sessionID, "error": waitErr})
			c.SendResponse(&compiler_service.ExecuteResponse{
				SessionId: c.sessionID,
				Payload: &compiler_service.ExecuteResponse_Error{
					Error: &compiler_service.Error{ErrorText: fmt.Sprintf("Execution error: %v", waitErr)},
				},
			})
		}
	} else {
		c.logger.Info("Execution completed successfully", map[string]any{"session_id": c.sessionID})
		c.SendResponse(&compiler_service.ExecuteResponse{
			SessionId: c.sessionID,
			Payload: &compiler_service.ExecuteResponse_Status{
				Status: &compiler_service.Status{State: "EXECUTION_COMPLETE"},
			},
		})
	}

	close(c.sendChan)
	c.logger.Info("Send channel closed", map[string]any{"session_id": c.sessionID})

	c.logger.Info("ExecutePython finished", map[string]any{"session_id": c.sessionID})
}

func (c *Client) CtxDone() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}
