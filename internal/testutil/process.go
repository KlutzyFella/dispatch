//go:build integration

package testutil

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

const childMainEnv = "DISPATCH_TEST_CHILD_MAIN"

// MainProcessChild lets a subprocess test execute the package's actual main.
func MainProcessChild() bool { return os.Getenv(childMainEnv) == "1" }

// MainProcess owns the compiled test subprocess, not a go-run wrapper.
type MainProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
}

// StartMainProcess re-executes the current test function. Its child branch must
// call main. Output is retained and dumped if the parent test fails.
func StartMainProcess(t *testing.T, overrides map[string]string) *MainProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	name := strings.Split(t.Name(), "/")[0]
	cmd := exec.Command(executable, "-test.run=^"+regexp.QuoteMeta(name)+"$", "-test.timeout=30s")
	env := make(map[string]string)
	for _, item := range os.Environ() {
		key, value, _ := strings.Cut(item, "=")
		env[key] = value
	}
	for key, value := range overrides {
		env[key] = value
	}
	env[childMainEnv] = "1"
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	logPath := filepath.Join(t.TempDir(), "process.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &MainProcess{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			cmd.Process.Kill()
			<-p.done
		}
		log.Close()
		if t.Failed() {
			t.Logf("subprocess output:\n%s", p.Logs(t))
		}
	})
	return p
}

func (p *MainProcess) Signal(signal os.Signal) error { return p.cmd.Process.Signal(signal) }

// Wait reports whether the subprocess exited before the observation deadline.
func (p *MainProcess) Wait(timeout time.Duration) (error, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return p.err, true
	case <-timer.C:
		return nil, false
	}
}

func (p *MainProcess) Logs(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// UnusedTCPPort releases a loopback port for a subprocess to bind. Startup
// failures are still checked: reservation does not make later binding atomic.
func UnusedTCPPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}
