package basic

import (
	"fmt"
	"net/http"

	"github.com/containous/traefik/provider/mesos/httpclient"
)

// Register registers a DoerFactory for HTTP Basic authentication
func Register() {
	httpclient.Register(httpclient.AuthBasic, httpclient.DoerFactory(func(cm httpclient.ConfigMap, c *http.Client) (doer httpclient.Doer) {
		obj := cm.FindOrPanic(httpclient.AuthBasic)
		config, ok := obj.(Credentials)
		if !ok {
			panic(fmt.Errorf("expected Credentials instead of %#+v", obj))
		}
		validate(config)
		if c != nil {
			doer = Doer(c, config)
		}
		return
	}))
}

func validate(c Credentials) {
	if c == (Credentials{}) {
		panic(ErrInvalidConfiguration)
	}
}

// Doer wraps an HTTP transactor given basic credentials
func Doer(client httpclient.Doer, credentials Credentials) httpclient.Doer {
	return httpclient.DoerFunc(func(req *http.Request) (*http.Response, error) {
		req.SetBasicAuth(credentials.Principal, credentials.Secret)
		return client.Do(req)
	})
}
