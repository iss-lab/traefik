package mesos

import (
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
)

func TestTaskRecords(t *testing.T) {
	var task = mesos.Task{
		SlaveID: "s_id",
		State:   "TASK_RUNNING",
	}
	var framework = mesos.Framework{
		Tasks: []mesos.Task{task},
	}
	var slave = mesos.Slave{
		ID:       "s_id",
		Hostname: "127.0.0.1",
	}

	var taskState = mesos.State{
		Slaves:     []mesos.Slave{slave},
		Frameworks: []mesos.Framework{framework},
	}

	var p = taskRecords(taskState)
	if len(p) == 0 {
		t.Fatal("No task")
	}
	if p[0].SlaveIP != slave.Hostname {
		t.Fatalf("The SlaveIP (%s) should be set with the slave hostname (%s)", p[0].SlaveID, slave.Hostname)
	}
}
