package mesos

import (
	"github.com/containous/traefik/provider/mesos/state"
	"github.com/containous/traefik/types"
)

func (p *Provider) buildConfiguration(tasks []state.Task) *types.Configuration {
	if p.TemplateVersion == 1 {
		return p.buildConfigurationV1(tasks)
	}
	return p.buildConfigurationV2(tasks)
}
