package mesos

import (
	"net"
	"strings"
	"sync"

	"github.com/containous/traefik/log"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
)

const (
	// dockerIPLabel is the key of the Label which holds the Docker containerizer IP value.
	dockerIPLabel = "Docker.NetworkSettings.IPAddress"
	// mesosIPLabel is the key of the label which holds the Mesos containerizer IP value.
	mesosIPLabel = "MesosContainerizer.NetworkSettings.IPAddress"
)

type StateCache struct {
	tasks    map[string]mesos.Task
	agentIPs map[string]string
	mutex    *sync.Mutex
}

func NewStateCache() *StateCache {
	taskMap := make(map[string]mesos.Task)
	agentMap := make(map[string]string)
	return &StateCache{
		tasks:    taskMap,
		agentIPs: agentMap,
		mutex:    &sync.Mutex{},
	}
}

func (c *StateCache) AddTasks(tasks []mesos.Task) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, t := range tasks {
		id := t.GetTaskID()
		c.tasks[id.GetValue()] = t
	}
}

func (c *StateCache) AddTask(t mesos.Task) {
	id := t.GetTaskID()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.tasks[id.GetValue()] = t
}

// UpdateTask updates a task in the cache
func (c *StateCache) UpdateTask(status mesos.TaskStatus) {
	id := status.GetTaskID()
	idVal := id.GetValue()
	state := status.GetState()
	agentID := status.GetAgentID()
	labels := status.GetLabels()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if t, ok := c.tasks[idVal]; ok {
		t.State = &state
		t.AgentID = *agentID
		t.Labels = labels
		t.Statuses = append(c.tasks[idVal].Statuses, status)
		c.tasks[idVal] = t
	}
}

func (c *StateCache) GetRunningTasks() []mesos.Task {
	tasks := []mesos.Task{}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, t := range c.tasks {
		if t.GetState() == mesos.TASK_RUNNING {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (c *StateCache) AddAgents(agents []master.Response_GetAgents_Agent) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, agent := range agents {
		pid := agent.GetPID()
		ip := getPidIP(pid)
		info := agent.GetAgentInfo()
		id := info.GetID()
		c.agentIPs[id.GetValue()] = ip
	}
}

func (c *StateCache) AddAgent(a master.Response_GetAgents_Agent) {
	info := a.GetAgentInfo()
	pid := a.GetPID()
	id := info.GetID()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.agentIPs[id.GetValue()] = getPidIP(pid)
}

func (c *StateCache) RemoveAgent(id string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.agentIPs, id)
}

func (c *StateCache) GetTaskIP(t mesos.Task, sources []string) string {
	var ip string
	for _, src := range sources {
		switch src {
		case "host":
			ip = c.hostIP(t)
		case "mesos":
			ip = mesosIP(t)
		case "docker":
			ip = dockerIP(t)
		case "netinfo":
			ip = networkInfoIP(t)
		}
		if srcIP := net.ParseIP(ip); len(srcIP) > 0 {
			return srcIP.String()
		}
	}
	return ""
}

func (c *StateCache) hostIP(t mesos.Task) string {
	id := t.GetAgentID()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if hostIP, ok := c.agentIPs[id.GetValue()]; ok {
		return hostIP
	}
	return ""
}

// networkInfoIPs returns IP address from a given Task's
// []Status.ContainerStatus.[]NetworkInfos.[]IPAddresses.IPAddress
func networkInfoIP(t mesos.Task) string {
	return statusIP(t.GetStatuses(), func(s *mesos.TaskStatus) string {
		containerStatus := s.GetContainerStatus()
		for _, netinfo := range containerStatus.GetNetworkInfos() {
			addrs := netinfo.GetIPAddresses()
			if len(addrs) > 0 {
				return addrs[0].GetIPAddress()
			}
		}
		return ""
	})
}

// dockerIP returns IP address from the values of all
// Task.[]Status.[]Labels whose keys are equal to "Docker.NetworkSettings.IPAddress".
func dockerIP(t mesos.Task) string {
	return statusIP(t.GetStatuses(), labelValue(dockerIPLabel))
}

// mesosIP returns IP address from the values of all
// Task.[]Status.[]Labels whose keys are equal to
// "MesosContainerizer.NetworkSettings.IPAddress".
func mesosIP(t mesos.Task) string {
	return statusIP(t.GetStatuses(), labelValue(mesosIPLabel))
}

// statusIP returns the latest running status IP extracted with the given src
func statusIP(st []mesos.TaskStatus, src func(*mesos.TaskStatus) string) string {
	// the state.json we extract from mesos makes no guarantees re: the order
	// of the task statuses so we should check the timestamps to avoid problems
	// down the line. we can't rely on seeing the same sequence. (@joris)
	// https://github.com/apache/mesos/blob/0.24.0/src/slave/slave.cpp#L5226-L5238
	ts, j := -1.0, -1
	for i := range st {
		if st[i].GetState() == mesos.TASK_RUNNING && st[i].GetTimestamp() > ts {
			ts, j = st[i].GetTimestamp(), i
		}
	}
	if j >= 0 {
		return src(&st[j])
	}
	return ""
}

// labelValue returns all given Status.[]Labels' values whose keys are equal
// to the given key
func labelValue(key string) func(*mesos.TaskStatus) string {
	return func(s *mesos.TaskStatus) string {
		lbls := s.GetLabels()
		for _, l := range lbls.GetLabels() {
			if l.GetKey() == key {
				return l.GetValue()
			}
		}
		return ""
	}
}

func getPidIP(pid string) string {
	splits := strings.Split(pid, "@")
	if len(splits) != 2 {
		log.Warnf("Unable to parse agent pid %s, skipping", pid)
		return ""
	}
	if _, err := net.ResolveTCPAddr("tcp", splits[1]); err != nil {
		log.Warnf("Unable to resolve ip from agent pid %s, skipping", pid)
		return ""
	}
	ip, _, err := net.SplitHostPort(splits[1])
	if err != nil {
		log.Warnf("Unable to split host from port in agent pid %s, skipping", pid)
		return ""
	}
	return ip
}
