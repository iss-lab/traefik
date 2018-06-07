package mesos

import (
	"testing"

	"github.com/containous/traefik/provider/label"
	"github.com/containous/traefik/types"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	"github.com/stretchr/testify/assert"
)

func TestHandleEvent(t *testing.T) {
	p := &Provider{
		Domain:           "docker.localhost",
		IPSources:        "host",
		ExposedByDefault: true,
		State:            NewStateCache(),
	}

	testCases := []struct {
		desc     string
		event    *master.Event
		expected map[string]types.Server
	}{
		{
			desc: "Subscribed with 3 agents, 3 tasks",
			event: &master.Event{
				Type: master.Event_SUBSCRIBED,
				Subscribed: &master.Event_Subscribed{
					GetState: &master.Response_GetState{
						GetAgents: &master.Response_GetAgents{
							Agents: []master.Response_GetAgents_Agent{
								anAgent("agent1",
									withAgentIP("10.10.10.11"),
								),
								anAgent("agent2",
									withAgentIP("10.10.10.12"),
								),
								anAgent("agent3",
									withAgentIP("10.10.10.13"),
								),
							},
						},
						GetTasks: &master.Response_GetTasks{
							Tasks: []mesos.Task{
								aTask("ID1",
									withTaskState(mesos.TASK_RUNNING),
									withAgentID("agent1"),
									withInfo("name1",
										withPorts(withPort("TCP", 80, "WEB"))),
									withStatus(withHealthy(true), withState(mesos.TASK_RUNNING)),
								),
								aTask("ID2",
									withTaskState(mesos.TASK_RUNNING),
									withAgentID("agent2"),
									withInfo("name1",
										withPorts(withPort("TCP", 80, "WEB"))),
									withStatus(withHealthy(true), withState(mesos.TASK_RUNNING)),
								),
								aTask("ID3",
									withTaskState(mesos.TASK_RUNNING),
									withAgentID("agent3"),
									withInfo("name1",
										withPorts(withPort("TCP", 80, "WEB"))),
									withStatus(withHealthy(true), withState(mesos.TASK_RUNNING)),
								),
							},
						},
					},
				},
			},
			expected: map[string]types.Server{
				"server-ID1": {
					URL:    "http://10.10.10.11:80",
					Weight: label.DefaultWeight,
				},
				"server-ID2": {
					URL:    "http://10.10.10.12:80",
					Weight: label.DefaultWeight,
				},
				"server-ID3": {
					URL:    "http://10.10.10.13:80",
					Weight: label.DefaultWeight,
				},
			},
		},
		{
			desc: "Task added with missing agent",
			event: &master.Event{
				Type: master.Event_TASK_ADDED,
				TaskAdded: &master.Event_TaskAdded{
					Task: aTask("ID4",
						withTaskState(mesos.TASK_RUNNING),
						withAgentID("agent4"),
						withInfo("name1",
							withPorts(withPort("TCP", 80, "WEB"))),
						withStatus(withHealthy(true), withState(mesos.TASK_RUNNING)),
					),
				},
			},
			expected: map[string]types.Server{
				"server-ID1": {
					URL:    "http://10.10.10.11:80",
					Weight: label.DefaultWeight,
				},
				"server-ID2": {
					URL:    "http://10.10.10.12:80",
					Weight: label.DefaultWeight,
				},
				"server-ID3": {
					URL:    "http://10.10.10.13:80",
					Weight: label.DefaultWeight,
				},
				"server-ID4": {
					URL:    "http://:80",
					Weight: label.DefaultWeight,
				},
			},
		},
		{
			desc: "Agent added",
			event: &master.Event{
				Type: master.Event_AGENT_ADDED,
				AgentAdded: &master.Event_AgentAdded{
					Agent: anAgent("agent4",
						withAgentIP("10.10.10.14"),
					),
				},
			},
			expected: map[string]types.Server{
				"server-ID1": {
					URL:    "http://10.10.10.11:80",
					Weight: label.DefaultWeight,
				},
				"server-ID2": {
					URL:    "http://10.10.10.12:80",
					Weight: label.DefaultWeight,
				},
				"server-ID3": {
					URL:    "http://10.10.10.13:80",
					Weight: label.DefaultWeight,
				},
				"server-ID4": {
					URL:    "http://10.10.10.14:80",
					Weight: label.DefaultWeight,
				},
			},
		},
		{
			desc: "Agent removed, task now missing host ip",
			event: &master.Event{
				Type: master.Event_AGENT_REMOVED,
				AgentRemoved: &master.Event_AgentRemoved{
					AgentID: mesos.AgentID{Value: "agent1"},
				},
			},
			expected: map[string]types.Server{
				"server-ID1": {
					URL:    "http://:80",
					Weight: label.DefaultWeight,
				},
				"server-ID2": {
					URL:    "http://10.10.10.12:80",
					Weight: label.DefaultWeight,
				},
				"server-ID3": {
					URL:    "http://10.10.10.13:80",
					Weight: label.DefaultWeight,
				},
				"server-ID4": {
					URL:    "http://10.10.10.14:80",
					Weight: label.DefaultWeight,
				},
			},
		},
		{
			desc: "Task updated, removing it from filtered tasks",
			event: &master.Event{
				Type: master.Event_TASK_UPDATED,
				TaskUpdated: &master.Event_TaskUpdated{
					Status: mesos.TaskStatus{
						TaskID:  mesos.TaskID{Value: "ID1"},
						State:   State(mesos.TASK_LOST),
						AgentID: &mesos.AgentID{Value: "agent1"},
						Labels:  &mesos.Labels{Labels: []mesos.Label{}},
					},
				},
			},
			expected: map[string]types.Server{
				"server-ID2": {
					URL:    "http://10.10.10.12:80",
					Weight: label.DefaultWeight,
				},
				"server-ID3": {
					URL:    "http://10.10.10.13:80",
					Weight: label.DefaultWeight,
				},
				"server-ID4": {
					URL:    "http://10.10.10.14:80",
					Weight: label.DefaultWeight,
				},
			},
		},
	}

	// Iterate over events so that the p.State hase the cumulative result
	for _, test := range testCases {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			p.handleEvent(*test.event)
			taskMap := p.filterTasks(p.State.GetRunningTasks())
			tasks := []taskData{}
			for _, ts := range taskMap {
				for _, t := range ts {
					tasks = append(tasks, t)
				}
			}
			actual := p.getServers(tasks)
			assert.Equal(t, test.expected, actual)
		})
	}
}
