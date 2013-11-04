package docker

import (
	"fmt"
	"github.com/dotcloud/docker/utils"
	"sync"
	"time"
)

type State struct {
	sync.RWMutex
	Running    bool
	ExitCode   int
	StartedAt  time.Time
	FinishedAt time.Time
}

// String returns a human-readable description of the state
func (s *State) String() string {
	s.RLock()
	defer s.RUnlock()

	if s.Running {
		return fmt.Sprintf("Up %s", utils.HumanDuration(time.Now().UTC().Sub(s.StartedAt)))
	}
	return fmt.Sprintf("Exit %d", s.ExitCode)
}

func (s *State) IsRunning() bool {
	s.RLock()
	defer s.RUnlock()

	return s.Running
}

func (s *State) GetExitCode() int {
	s.RLock()
	defer s.RUnlock()

	return s.ExitCode
}

func (s *State) SetRunning() {
	s.Lock()
	defer s.Unlock()

	s.Running = true
	s.ExitCode = 0
	s.StartedAt = time.Now().UTC()
}

func (s *State) SetStopped(exitCode int) {
	s.Lock()
	defer s.Unlock()

	s.Running = false
	s.FinishedAt = time.Now().UTC()
	s.ExitCode = exitCode
}
