package iam

import (
	"errors"

	"github.com/containous/traefik/provider/mesos/httpclient"
)

// ErrInvalidConfiguration generated when Config has missing or invalid data
var ErrInvalidConfiguration = errors.New("invalid HTTP IAM configuration")

// Config captures the configuration that allows mesos-dns to authenticate against some
// IAM endpoint.
type Config struct {
	ID            string `json:"uid"`            // ID
	PrivateKey    string `json:"private_key"`    // PrivateKey
	LoginEndpoint string `json:"login_endpoint"` // LoginEndpoint
}

// Configuration returns a functional option for an httpclient.ConfigMap
func Configuration(c Config) httpclient.ConfigMapOption {
	return func(cm httpclient.ConfigMap) {
		cm[httpclient.AuthIAM] = c
	}
}
