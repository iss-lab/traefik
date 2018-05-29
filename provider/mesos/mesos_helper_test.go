package mesos

import (
	"strings"
	"testing"

	"github.com/containous/traefik/provider/label"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/stretchr/testify/assert"
)

// test helpers

func TestBuilder(t *testing.T) {
	result := aTask("ID1",
		withIP("10.10.10.10"),
		withLabel("foo", "bar"),
		withLabel("fii", "bar"),
		withLabel("fuu", "bar"),
		withInfo("name1",
			withPorts(withPort("TCP", 80, "p"),
				withPortTCP(81, "n"))),
		withStatus(withHealthy(true), withState("a")))

	expected := mesos.Task{
		FrameworkID: "",
		ID:          "ID1",
		SlaveIP:     "10.10.10.10",
		Name:        "",
		SlaveID:     "",
		State:       "",
		Statuses: []mesos.Status{{
			State:           "a",
			Healthy:         Bool(true),
			ContainerStatus: mesos.ContainerStatus{},
		}},
		DiscoveryInfo: mesos.DiscoveryInfo{
			Name: "name1",
			Labels: struct {
				Labels []mesos.Label "json:\"labels\""
			}{},
			Ports: mesos.Ports{DiscoveryPorts: []mesos.DiscoveryPort{
				{Protocol: "TCP", Number: 80, Name: "p"},
				{Protocol: "TCP", Number: 81, Name: "n"}}}},
		Labels: []mesos.Label{
			{Key: "foo", Value: "bar"},
			{Key: "fii", Value: "bar"},
			{Key: "fuu", Value: "bar"}}}

	assert.Equal(t, expected, result)
}

func aTaskData(id, segment string, ops ...func(*mesos.Task)) taskData {
	ts := &mesos.Task{ID: id}
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
	ts := &mesos.Task{ID: id}
	for _, op := range ops {
		op(ts)
	}
	return *ts
}

func withIP(ip string) func(*mesos.Task) {
	return func(task *mesos.Task) {
		task.SlaveIP = ip
	}
}

func withInfo(name string, ops ...func(*mesos.DiscoveryInfo)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		info := &mesos.DiscoveryInfo{Name: name}
		for _, op := range ops {
			op(info)
		}
		task.DiscoveryInfo = *info
	}
}

func withPorts(ops ...func(port *mesos.DiscoveryPort)) func(*mesos.DiscoveryInfo) {
	return func(info *mesos.DiscoveryInfo) {
		var ports []mesos.DiscoveryPort
		for _, op := range ops {
			pt := &mesos.DiscoveryPort{}
			op(pt)
			ports = append(ports, *pt)
		}

		info.Ports = mesos.Ports{
			DiscoveryPorts: ports,
		}
	}
}

func withPort(proto string, port int, name string) func(port *mesos.DiscoveryPort) {
	return func(p *mesos.DiscoveryPort) {
		p.Protocol = proto
		p.Number = port
		p.Name = name
	}
}

func withPortTCP(port int, name string) func(port *mesos.DiscoveryPort) {
	return withPort("TCP", port, name)
}

func withStatus(ops ...func(*mesos.Status)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		st := &mesos.Status{}
		for _, op := range ops {
			op(st)
		}
		task.Statuses = append(task.Statuses, *st)
	}
}
func withDefaultStatus(ops ...func(*mesos.Status)) func(*mesos.Task) {
	return func(task *mesos.Task) {
		for _, op := range ops {
			st := &mesos.Status{
				State:   "TASK_RUNNING",
				Healthy: Bool(true),
			}
			op(st)
			task.Statuses = append(task.Statuses, *st)
		}
	}
}

func withHealthy(st bool) func(*mesos.Status) {
	return func(status *mesos.Status) {
		status.Healthy = Bool(st)
	}
}

func withState(st string) func(*mesos.Status) {
	return func(status *mesos.Status) {
		status.State = st
	}
}

func withLabel(key, value string) func(*mesos.Task) {
	return func(task *mesos.Task) {
		lbl := mesos.Label{Key: key, Value: value}
		task.Labels = append(task.Labels, lbl)
	}
}

func withSegmentLabel(key, value, segmentName string) func(*mesos.Task) {
	if len(segmentName) == 0 {
		panic("segmentName can not be empty")
	}

	property := strings.TrimPrefix(key, label.Prefix)
	return func(task *mesos.Task) {
		lbl := mesos.Label{Key: label.Prefix + segmentName + "." + property, Value: value}
		task.Labels = append(task.Labels, lbl)
	}
}

func Bool(v bool) *bool {
	return &v
}
