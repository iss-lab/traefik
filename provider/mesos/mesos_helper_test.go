package mesos

import (
	"strings"
	"testing"

	"github.com/containous/traefik/provider/label"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	"github.com/stretchr/testify/assert"
)

// test helpers

func TestBuilder(t *testing.T) {
	result := aTask("ID1",
		withNetIP("10.10.10.10"),
		withLabel("foo", "bar"),
		withLabel("fii", "bar"),
		withLabel("fuu", "bar"),
		withInfo("name1",
			withPorts(withPort("TCP", 80, "p"),
				withPortTCP(81, "n"))),
		withStatus(withHealthy(true), withState(mesos.TASK_RUNNING)))
	expected := mesos.Task{
		FrameworkID: mesos.FrameworkID{
			Value: "",
		},
		TaskID: mesos.TaskID{
			"ID1",
		},
		Name: "",
		AgentID: mesos.AgentID{
			Value: "",
		},
		State: nil,
		Statuses: []mesos.TaskStatus{{
			State:   State(mesos.TASK_RUNNING),
			Healthy: Bool(true),
			ContainerStatus: &mesos.ContainerStatus{
				NetworkInfos: []mesos.NetworkInfo{{
					IPAddresses: []mesos.NetworkInfo_IPAddress{{
						IPAddress: String("10.10.10.10"),
					}}}}},
		}},
		Discovery: &mesos.DiscoveryInfo{
			Name:   String("name1"),
			Labels: nil,
			Ports: &mesos.Ports{Ports: []mesos.Port{
				{Protocol: String("TCP"), Number: 80, Name: String("p")},
				{Protocol: String("TCP"), Number: 81, Name: String("n")}}},
		},
		Labels: &mesos.Labels{
			Labels: []mesos.Label{
				{Key: "foo", Value: String("bar")},
				{Key: "fii", Value: String("bar")},
				{Key: "fuu", Value: String("bar")},
			}}}

	assert.Equal(t, expected, result)
}

func anAgent(id string, ops ...func(*master.Response_GetAgents_Agent)) master.Response_GetAgents_Agent {
	agent := &master.Response_GetAgents_Agent{
		AgentInfo: mesos.AgentInfo{
			ID: &mesos.AgentID{
				Value: id,
			},
		},
	}
	for _, op := range ops {
		op(agent)
	}
	return *agent
}

func withAgentIP(ip string) func(*master.Response_GetAgents_Agent) {
	return func(a *master.Response_GetAgents_Agent) {
		a.PID = String("slave(1)@" + ip + ":5051")
	}
}

func aTaskData(id, segment string, ops ...func(*mesos.Task)) taskData {
	ts := &mesos.Task{TaskID: mesos.TaskID{Value: id}}
	for _, op := range ops {
		op(ts)
	}
	lbls := label.ExtractTraefikLabels(extractLabels(*ts))
	if len(lbls[segment]) > 0 {
		return taskData{Task: *ts, TraefikLabels: lbls[segment], SegmentName: segment}
	}
	return taskData{Task: *ts, TraefikLabels: lbls[""], SegmentName: segment}
}

func segmentedTaskData(segments []string, ts mesos.Task) []taskData {
	td := []taskData{}
	lbls := label.ExtractTraefikLabels(extractLabels(ts))
	for _, s := range segments {
		if l, ok := lbls[s]; !ok {
			td = append(td, taskData{Task: ts, TraefikLabels: lbls[""], SegmentName: s})
		} else {
			td = append(td, taskData{Task: ts, TraefikLabels: l, SegmentName: s})
		}
	}
	return td
}

func aTask(id string, ops ...func(*mesos.Task)) mesos.Task {
	ts := &mesos.Task{TaskID: mesos.TaskID{Value: id}}
	for _, op := range ops {
		op(ts)
	}
	return *ts
}

func withNetIP(ip string) func(*mesos.Task) {
	return withStatus(withStatusNetIP(ip))
}

func withDockerIP(ip string) func(*mesos.Task) {
	return withStatus(withStatusDockerIP(ip))
}

func withMesosIP(ip string) func(*mesos.Task) {
	return withStatus(withStatusMesosIP(ip))
}

func withAgentID(id string) func(*mesos.Task) {
	return func(task *mesos.Task) {
		task.AgentID = mesos.AgentID{Value: id}
	}
}

func withInfo(name string, ops ...func(*mesos.DiscoveryInfo)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		info := &mesos.DiscoveryInfo{Name: String(name)}
		for _, op := range ops {
			op(info)
		}
		task.Discovery = info
	}
}

func withPorts(ops ...func(port *mesos.Port)) func(*mesos.DiscoveryInfo) {
	return func(info *mesos.DiscoveryInfo) {
		var ports []mesos.Port
		for _, op := range ops {
			pt := &mesos.Port{}
			op(pt)
			ports = append(ports, *pt)
		}

		info.Ports = &mesos.Ports{
			Ports: ports,
		}
	}
}

func withPort(proto string, port int, name string) func(port *mesos.Port) {
	return func(p *mesos.Port) {
		p.Protocol = String(proto)
		p.Number = uint32(port)
		p.Name = String(name)
	}
}

func withPortTCP(port int, name string) func(port *mesos.Port) {
	return withPort("TCP", port, name)
}

func withTaskState(st mesos.TaskState) func(*mesos.Task) {
	return func(task *mesos.Task) {
		task.State = &st
	}
}

func withStatus(ops ...func(*mesos.TaskStatus)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		var st *mesos.TaskStatus
		if len(task.Statuses) == 0 {
			st = &mesos.TaskStatus{}
			task.Statuses = append(task.Statuses, *st)
		} else {
			st = &task.Statuses[0]
		}
		for _, op := range ops {
			op(st)
		}
		task.Statuses = []mesos.TaskStatus{*st}
	}
}
func withDefaultStatus(ops ...func(*mesos.TaskStatus)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		for _, op := range ops {
			st := &mesos.TaskStatus{
				State:   State(mesos.TASK_RUNNING),
				Healthy: Bool(true),
			}
			op(st)
			task.Statuses = append(task.Statuses, *st)
		}
	}
}

func withStatusNetIP(st string) func(*mesos.TaskStatus) {
	return func(status *mesos.TaskStatus) {
		status.ContainerStatus = &mesos.ContainerStatus{
			NetworkInfos: []mesos.NetworkInfo{{
				IPAddresses: []mesos.NetworkInfo_IPAddress{{
					IPAddress: String(st),
				}}},
			},
		}
	}
}

func withStatusDockerIP(st string) func(*mesos.TaskStatus) {
	return withStatusLabelIP(st, dockerIPLabel)
}

func withStatusMesosIP(st string) func(*mesos.TaskStatus) {
	return withStatusLabelIP(st, mesosIPLabel)
}

func withStatusLabelIP(st, lbl string) func(*mesos.TaskStatus) {
	return func(status *mesos.TaskStatus) {
		status.Labels = &mesos.Labels{
			Labels: []mesos.Label{{
				Key:   lbl,
				Value: String(st),
			}},
		}
	}
}

func withHealthy(st bool) func(*mesos.TaskStatus) {
	return func(status *mesos.TaskStatus) {
		status.Healthy = Bool(st)
	}
}

func withState(st mesos.TaskState) func(*mesos.TaskStatus) {
	return func(status *mesos.TaskStatus) {
		status.State = &st
	}
}

func withLabel(key, value string) func(*mesos.Task) {
	return func(task *mesos.Task) {
		lbl := mesos.Label{Key: key, Value: &value}
		if task.Labels == nil {
			task.Labels = &mesos.Labels{Labels: []mesos.Label{}}
		}
		task.Labels.Labels = append(task.Labels.Labels, lbl)
	}
}

func withSegmentLabel(key, value, segmentName string) func(*mesos.Task) {
	if len(segmentName) == 0 {
		panic("segmentName can not be empty")
	}

	property := strings.TrimPrefix(key, label.Prefix)
	return func(task *mesos.Task) {
		lbl := mesos.Label{Key: label.Prefix + segmentName + "." + property, Value: &value}
		if task.Labels == nil {
			task.Labels = &mesos.Labels{Labels: []mesos.Label{}}
		}
		task.Labels.Labels = append(task.Labels.Labels, lbl)
	}
}

func Bool(v bool) *bool {
	return &v
}

func String(v string) *string {
	return &v
}

func State(v mesos.TaskState) *mesos.TaskState {
	return &v
}
