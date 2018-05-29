package mesos

import (
	"github.com/containous/traefik/types"
	"github.com/mesos/mesos-go/api/v1/lib"
)

func (p *Provider) buildConfiguration(tasks []mesos.Task) *types.Configuration {
	if p.TemplateVersion == 1 {
		return p.buildConfigurationV1(tasks)
	}
	return p.buildConfigurationV2(tasks)
}
