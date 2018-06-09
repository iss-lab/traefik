package mesos

import (
	"time"

	"github.com/cenk/backoff"
	"github.com/containous/traefik/job"
	"github.com/containous/traefik/log"
	"github.com/containous/traefik/provider"
	"github.com/containous/traefik/safe"
	"github.com/containous/traefik/types"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
)

var _ provider.Provider = (*Provider)(nil)

//Provider holds configuration of the provider.
type Provider struct {
	provider.BaseProvider
	Endpoint           string `description:"Mesos server endpoint. You can also specify multiple endpoint for Mesos"`
	Domain             string `description:"Default domain used"`
	ExposedByDefault   bool   `description:"Expose Mesos apps by default" export:"true"`
	GroupsAsSubDomains bool   `description:"Convert Mesos groups to subdomains" export:"true"`
	ZkDetectionTimeout int    `description:"Zookeeper timeout (in seconds)" export:"true"`
	RefreshSeconds     int    `description:"Polling interval (in seconds)" export:"true"`
	IPSources          string `description:"IPSources (e.g. host, docker, mesos, netinfo)" export:"true"`
	StateTimeoutSecond int    `description:"HTTP Timeout (in seconds)" export:"true"`

	// Internal
	State *StateCache
}

// Provide allows the mesos provider to provide configurations to traefik
// using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- types.ConfigMessage, pool *safe.Pool, constraints types.Constraints) error {
	mkClient := MkClientFn(p.Endpoint, p.ZkDetectionTimeout, p.StateTimeoutSecond)

	pool.Go(func(stop chan bool) {
		operation := func() (err error) {
			var cli *Client
			cli, err = mkClient()
			if err != nil {
				return
			}
			p.State = NewStateCache()

			eventCh, errCh := cli.Watch()
			for {
				select {
				case <-stop:
					cli.Stop()
					return
				case e := <-eventCh:
					if changed := p.handleEvent(e); changed {
						tasks := p.State.GetRunningTasks()
						configuration := p.buildConfiguration(tasks)
						if configuration != nil {
							configurationChan <- types.ConfigMessage{
								ProviderName:  "mesos",
								Configuration: configuration,
							}
						}
					}
					if !p.Watch {
						cli.Stop()
						return
					}
				case err = <-errCh:
					log.Errorf("%s", err)
					return
				}
			}
			return
		}

		notify := func(err error, time time.Duration) {
			log.Errorf("Provider connection error: %s; retrying in %s", err, time)
		}
		err := backoff.RetryNotify(safe.OperationWithRecover(operation), job.NewBackOff(backoff.NewExponentialBackOff()), notify)
		if err != nil {
			log.Errorf("Cannot connect to Provider: %s", err)
		}
	})

	return nil
}

func (p *Provider) handleEvent(e master.Event) (changed bool) {
	log.Debugf("Got mesos master event: %s", e.GetType())
	switch t := e.GetType(); t {
	case master.Event_SUBSCRIBED:
		agents := e.GetSubscribed().GetGetState().GetGetAgents().GetAgents()
		// Agents must be added first because cachedTask.AgentIP needs them
		p.State.AddAgents(agents)
		tasks := e.GetSubscribed().GetGetState().GetGetTasks().GetTasks()
		p.State.AddTasks(tasks)
		changed = true
	case master.Event_TASK_ADDED:
		task := e.GetTaskAdded().GetTask()
		p.State.AddTask(task)
		if task.GetState() == mesos.TASK_RUNNING {
			changed = true
		}
	case master.Event_TASK_UPDATED:
		task := e.GetTaskUpdated().GetStatus()
		p.State.UpdateTask(task)
		changed = true
	case master.Event_AGENT_ADDED:
		agent := e.GetAgentAdded().GetAgent()
		p.State.AddAgent(agent)
	case master.Event_AGENT_REMOVED:
		id := e.GetAgentRemoved().GetAgentID()
		p.State.RemoveAgent(id.GetValue())
	case master.Event_HEARTBEAT:
		// noop, handled in client
	default:
		// noop
	}
	return changed
}
