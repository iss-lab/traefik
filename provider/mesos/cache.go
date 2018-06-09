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
	// dockerIPLabel is the key of the Label which holds the Docker containerizer IP value. Set by Marathon.
	dockerIPLabel = "Docker.NetworkSettings.IPAddress"
	// mesosIPLabel is the key of the label which holds the Mesos containerizer IP value. Set by Marathon.
	mesosIPLabel = "MesosContainerizer.NetworkSettings.IPAddress"
	// networkInfoIPLabel is the key of the label which holds the Container's NetworkInfo IP value. Set by this package.
	networkInfoIPLabel = "ContainerStatus.NetworkInfo.IPAddress"
	// networkInfoIPLabel is the key of the label which holds the Mesos agent IP value. Set by this package.
	agentIPLabel = "Mesos.Agent.IPAddress"
)

var (
	terminalStates = [...]mesos.TaskState{mesos.TASK_FINISHED, mesos.TASK_FAILED, mesos.TASK_KILLED, mesos.TASK_ERROR}
	ipSourceLabels = map[string]string{
		"host":    agentIPLabel,
		"mesos":   mesosIPLabel,
		"docker":  dockerIPLabel,
		"netinfo": networkInfoIPLabel,
	}
)

// CachedTask contains a task and linked agent ip address
type CachedTask struct {
	mesos.Task
	LabelMap map[string]string
}

// StateCache a managed cache with maps to store tasks and agent ips
type StateCache struct {
	tasks    map[string]CachedTask // all tasks, key:TaskID
	agentIPs map[string]string     // all agent IPs, key:AgentID
	lock     sync.RWMutex
}

func NewStateCache() *StateCache {
	taskMap := make(map[string]mesos.Task)
	agentMap := make(map[string]string)
	return &StateCache{
		tasks:    taskMap,
		agentIPs: agentMap,
		lock:     &sync.RWMutex{},
	}
}

func (c *StateCache) AddTasks(tasks []mesos.Task) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, t := range tasks {
		agentIP := label.GetStringValue(c.agentIPs, t.GetAgentID().GetValue(), "")
		c.tasks[t.GetTaskID().GetValue()] = newCachedTask(t, agentIP)
	}
}

func (c *StateCache) AddTask(t mesos.Task) {
	c.lock.Lock()
	defer c.lock.Unlock()
	agentIP := label.GetStringValue(c.agentIPs, t.GetAgentID().GetValue(), "")
	c.tasks[t.GetTaskID().GetValue()] = newCachedTask(t, agentIP)
}

// UpdateTask updates a task in the cache
func (c *StateCache) UpdateTask(status mesos.TaskStatus) {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskID := status.GetTaskID()
	state := status.GetState()
	executorID := status.GetExecutorID()
	agentID := status.GetAgentID()
	uuid := status.GetUUID()

	ct, ok := c.tasks[taskID.GetValue()]
	// TODO: If we encounter an update
	if !ok {
		log.Warningf("Recieved update for unknown task %s, ignoring", taskID.GetValue())
		return
	}
	// If the task has a terminal state, it can be removed from cache
	if terminalState(state) {
		c.RemoveTask(idVal)
		return
	}

	// Update stale task values from new status, labels can be excluded because
	// LabelMap has merged values from latest status
	ct.Statuses = append(ct.Statuses, status)
	ct.ExecutorID = &executorID
	ct.AgentID = *agentID
	ct.State = &state
	ct.StatusUpdateState = &state
	ct.StatusUpdateUUID = uuid

	// Set the new agentIP if not blank, default to oldAgentIP
	oldAgentIP := label.GetStringValue(ct.LabelMap, agentIPLabel, "")
	agentIP := label.GetStringValue(c.agentIPs, agentID.GetValue(), oldAgentIP)

	// Create a new cached task with merged labels and updated IP addresses
	c.tasks[taskID.GetValue()] = newCachedTask(ct.Task, agentIP)
}

func (c *StateCache) RemoveTask(id string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.tasks, id)
}

func (c *StateCache) GetRunningTasks() []CachedTask {
	tasks := []CachedTask{}
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, t := range c.tasks {
		if t.GetState() == mesos.TASK_RUNNING {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (c *StateCache) AddAgents(agents []master.Response_GetAgents_Agent) {
	c.lock.Lock()
	defer c.lock.Unlock()
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
	c.lock.Lock()
	defer c.lock.Unlock()
	c.agentIPs[id.GetValue()] = getPidIP(pid)
}

func (c *StateCache) RemoveAgent(id string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.agentIPs, id)
}

func GetTaskHealthy(t CachedTask) bool {
	if latest := latestStatus(t.GetStatuses()); latest != nil {
		if latest.Healthy != nil {
			return latest.GetHealthy()
		}
		// If there is a running task with a nil Healthy, assume true
		return true
	}
	// Task not running
	return false
}

func GetTaskIP(t CachedTask, sources []string) string {
	for _, src := range sources {
		if lbl, ok := ipSourceLabels[src]; ok {
			ip := label.GetStringValue(t.LabelMap, lbl, "")
			if srcIP := net.ParseIP(ip); len(srcIP) > 0 {
				return srcIP.String()
			}
		}
	}
	return ""
}

func newCachedTask(t mesos.Task, agentIP string) CachedTask {
	status := latestStatus(t.GetStatuses())
	taskLabels := extractLabels(t.GetLabels())
	statusLabels := extractLabels(status.GetLabels())

	// The latest status labels override task labels during update
	for k, v := range statusLabels {
		taskLabels[k] = v
	}

	// Set additional ip source labels
	taskLabels[agentIPLabel] = agentIP
	taskLabels[networkInfoIPLabel] = networkInfoIP(status)

	ct := CachedTask{
		Task:     t,
		LabelMap: taskLabels,
	}
}

// func (c *StateCache) hostIP(t mesos.Task) string {
// 	id := t.GetAgentID()
// 	c.lock.Lock()
// 	defer c.lock.Unlock()
// 	if hostIP, ok := c.agentIPs[id.GetValue()]; ok {
// 		return hostIP
// 	}
// 	return ""
// }

// networkInfoIPs returns IP address from a given Task's
// []Status.ContainerStatus.[]NetworkInfos.[]IPAddresses.IPAddress
func networkInfoIP(status mesos.TaskStatus) string {
	containerStatus := status.GetContainerStatus()
	for _, netinfo := range containerStatus.GetNetworkInfos() {
		addrs := netinfo.GetIPAddresses()
		if len(addrs) > 0 {
			return addrs[0].GetIPAddress()
		}
	}
	return ""
}

// // dockerIP returns IP address from the values of all
// // Task.[]Status.[]Labels whose keys are equal to "Docker.NetworkSettings.IPAddress".
// func dockerIP(t mesos.Task) string {
// 	return
// }

// // mesosIP returns IP address from the values of all
// // Task.[]Status.[]Labels whose keys are equal to
// // "MesosContainerizer.NetworkSettings.IPAddress".
// func mesosIP(t mesos.Task) string {
// 	return
// }

func latestStatus(st []mesos.TaskStatus) *mesos.TaskStatus {
	ts, j := -1.0, -1
	for i := range st {
		if st[i].GetState() == mesos.TASK_RUNNING && st[i].GetTimestamp() > ts {
			ts, j = st[i].GetTimestamp(), i
		}
	}
	if j >= 0 {
		return &st[j]
	}
	return nil
}

// // statusIP returns the latest running status IP extracted with the given src
// func statusIP(st []mesos.TaskStatus, src func(*mesos.TaskStatus) string) string {
// 	// there are no guarantees about the order of the task statuses so we should
// 	// check the timestamps
// 	if latest := latestStatus(st); latest != nil {
// 		return src(latest)
// 	}
// 	return ""
// }

// labelValue returns all given Status.[]Labels' values whose keys are equal
// to the given key
// func labelValue(key string) func(*mesos.TaskStatus) string {
// 	return func(s *mesos.TaskStatus) string {
// 		lbls := s.GetLabels()
// 		for _, l := range lbls.GetLabels() {
// 			if l.GetKey() == key {
// 				return l.GetValue()
// 			}
// 		}
// 		return ""
// 	}
// }

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

func terminalState(state mesos.TaskState) bool {
	for _, s := range terminalStates {
		if state == s {
			return true
		}
	}
	return false
}

func extractLabels(lbls mesos.Labels) map[string]string {
	labels := make(map[string]string)
	for _, lbl := range lbls.GetLabels() {
		labels[lbl.Key] = lbl.GetValue()
	}
	return labels
}
