package basic

import (
	"errors"

	"github.com/containous/traefik/provider/mesos/httpclient"
)

// ErrInvalidConfiguration generated when Credentials has missing or invalid data
var ErrInvalidConfiguration = errors.New("invalid HTTP Basic configuration")

// Credentials holds a mesos-master principal / secret combination
type Credentials struct {
	Principal string
	Secret    string
}

// Configuration returns a functional option for an httpclient.ConfigMap
func Configuration(c Credentials) httpclient.ConfigMapOption {
	return func(cm httpclient.ConfigMap) {
		cm[httpclient.AuthBasic] = c
	}
}
