package mesos

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cenk/backoff"
	"github.com/containous/traefik/job"
	"github.com/containous/traefik/log"
	"github.com/containous/traefik/provider"
	"github.com/containous/traefik/provider/mesos/detect"
	"github.com/containous/traefik/provider/mesos/httpclient"
	"github.com/containous/traefik/provider/mesos/httpclient/basic"
	"github.com/containous/traefik/provider/mesos/httpclient/iam"
	"github.com/containous/traefik/provider/mesos/httpclient/urls"
	"github.com/containous/traefik/provider/mesos/state"
	"github.com/containous/traefik/safe"
	"github.com/containous/traefik/types"

	// Register mesos zoo the detector
	"github.com/mesos/mesos-go/detector"
	_ "github.com/mesos/mesos-go/detector/zoo"
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
	HTTPSOn            bool   `description:"Communicate with Mesos using HTTPS if set to true"`
	CACertFile         string `description:"CA certificate to use to verify Mesos Master certificate"`
	CertFile           string `description:"CA certificate to use to verify Mesos Master certificate"`
	KeyFile            string `description:"Client certificate key to use"`
	IAMConfigFile      string `description:"IAM Config File"`
	Principal          string `description:"Basic credenital principal for Mesos"`
	Secret             string `description:"Basic credenital secret for Mesos"`

	CAPool         *x509.CertPool
	Cert           tls.Certificate
	Authentication httpclient.AuthMechanism
	Credentials    basic.Credentials
	HTTPConfigMap  httpclient.ConfigMap
	Masters        []string
}

// Provide allows the mesos provider to provide configurations to traefik
// using the given configuration channel.
func (p *Provider) Provide(configurationChan chan<- types.ConfigMessage, pool *safe.Pool, constraints types.Constraints) error {
	operation := func() error {
		log.Debugf("%s", p.IPSources)

		// initialize http options
		p.Authentication = httpclient.AuthNone
		creds := new(basic.Credentials)
		creds.Principal = p.Principal
		creds.Secret = p.Secret
		p.Credentials = *creds
		p.initCertificates()
		p.initAuthentication()

		var zk string
		var masters []string

		if strings.HasPrefix(p.Endpoint, "zk://") {
			zk = p.Endpoint
		} else {
			masters = strings.Split(p.Endpoint, ",")
		}

		errch := make(chan error)

		changed := detectMasters(zk, masters)
		reload := time.NewTicker(time.Second * time.Duration(p.RefreshSeconds))
		zkTimeout := time.Second * time.Duration(p.ZkDetectionTimeout)
		timeout := time.AfterFunc(zkTimeout, func() {
			if zkTimeout > 0 {
				errch <- fmt.Errorf("master detection timed out after %s", zkTimeout)
			}
		})

		defer reload.Stop()
		defer detect.HandleCrash()

		if !p.Watch {
			reload.Stop()
			timeout.Stop()
		}

		for {
			select {
			case <-reload.C:
				tasks := p.getTasks()
				configuration := p.buildConfiguration(tasks)
				if configuration != nil {
					configurationChan <- types.ConfigMessage{
						ProviderName:  "mesos",
						Configuration: configuration,
					}
				}
			case masters := <-changed:
				if len(masters) == 0 || masters[0] == "" {
					// no leader
					timeout.Reset(zkTimeout)
				} else {
					timeout.Stop()
				}
				log.Debugf("new masters detected: %v", masters)
				p.Masters = masters
				tasks := p.getTasks()
				configuration := p.buildConfiguration(tasks)
				if configuration != nil {
					configurationChan <- types.ConfigMessage{
						ProviderName:  "mesos",
						Configuration: configuration,
					}
				}
			case err := <-errch:
				log.Errorf("%s", err)
			}
		}
	}

	notify := func(err error, time time.Duration) {
		log.Errorf("Mesos connection error %+v, retrying in %s", err, time)
	}
	err := backoff.RetryNotify(safe.OperationWithRecover(operation), job.NewBackOff(backoff.NewExponentialBackOff()), notify)
	if err != nil {
		log.Errorf("Cannot connect to Mesos server %+v", err)
	}
	return nil
}

func detectMasters(zk string, masters []string) <-chan []string {
	changed := make(chan []string, 1)
	if zk != "" {
		log.Debugf("Starting master detector for ZK ", zk)
		if md, err := detector.New(zk); err != nil {
			log.Errorf("Failed to create master detector: %v", err)
		} else if err := md.Detect(detect.NewMasters(masters, changed)); err != nil {
			log.Errorf("Failed to initialize master detector: %v", err)
		}
	} else {
		changed <- masters
	}
	return changed
}

func (p *Provider) getTasks() []state.Task {
	var opt, tlsClientConfig = httpclient.TLSConfig(p.HTTPSOn, p.CAPool, p.Cert)
	var transport = httpclient.Transport(&http.Transport{
		DisableKeepAlives:   true, // Mesos master doesn't implement defensive HTTP
		MaxIdleConnsPerHost: 2,
		TLSClientConfig:     tlsClientConfig,
	})
	var timeout = httpclient.Timeout(time.Duration(p.StateTimeoutSecond) * time.Second)
	var doer = httpclient.New(p.Authentication, p.HTTPConfigMap, transport, timeout)
	var stateEndpoint = urls.Builder{}.With(
		urls.Path("/master/state.json"),
		opt,
	)
	loader := state.NewStateLoader(doer, stateEndpoint, func(b []byte, v *state.State) error {
		return json.Unmarshal(b, v)
	})

	st, err := loader(p.Masters)
	if err != nil {
		log.Errorf("Failed to create a client for Mesos, error: %v", err)
		return nil
	}
	if st.Leader == "" {
		log.Error("Failed to create a client for Mesos, error: empty master")
		return nil
	}

	return taskRecords(st)
}

func taskRecords(st state.State) []state.Task {
	var tasks []state.Task
	allSlaveIPs := map[string][]string{}

	for _, s := range st.Slaves {
		slaveIPs := []string{}
		if ips := hostToIPs(s.PID.Host); len(ips) > 0 {
			for _, ip := range ips {
				slaveIPs = append(slaveIPs, ip.String())
			}
		} else {
			log.Debugf("string %q for slave with id %q is not a valid IP address", s.PID.Host, s.ID)
		}
		if len(slaveIPs) == 0 {
			slaveIPs = append(slaveIPs, s.PID.Host)
		}
		allSlaveIPs[s.ID] = slaveIPs
	}

	for _, f := range st.Frameworks {
		for _, task := range f.Tasks {
			var ok bool
			task.SlaveIPs, ok = allSlaveIPs[task.SlaveID]

			// only do running and discoverable tasks
			if ok && (task.State == "TASK_RUNNING") {
				tasks = append(tasks, task)
			}
		}
	}

	return tasks
}

// hostToIPs attempts to parse a hostname into an ip.
// If that doesn't work it will perform a lookup and try to
// find one ipv4 and one ipv6 in the results.
func hostToIPs(hostname string) (ips []net.IP) {
	if ip := net.ParseIP(hostname); ip != nil {
		ips = []net.IP{ip}
	} else if allIPs, err := net.LookupIP(hostname); err == nil {
		ips = ipsTo4And6(allIPs)
	}
	if len(ips) == 0 {
		log.Debugf("cannot translate hostname %q into an ipv4 or ipv6 address", hostname)
	}
	return
}

// ipsTo4And6 returns a list with at most 1 ipv4 and 1 ipv6
// from a list of IPs
func ipsTo4And6(allIPs []net.IP) (ips []net.IP) {
	var ipv4, ipv6 net.IP
	for _, ip := range allIPs {
		if ipv4 != nil && ipv6 != nil {
			break
		} else if t4 := ip.To4(); t4 != nil {
			if ipv4 == nil {
				ipv4 = t4
			}
		} else if t6 := ip.To16(); t6 != nil {
			if ipv6 == nil {
				ipv6 = t6
			}
		}
	}
	ips = []net.IP{}
	if ipv4 != nil {
		ips = append(ips, ipv4)
	}
	if ipv6 != nil {
		ips = append(ips, ipv6)
	}
	return
}

func (p *Provider) initCertificates() {
	if p.CACertFile != "" {
		pool, err := readCACertFile(p.CACertFile)
		if err != nil {
			log.Error(err)
		}
		p.CAPool = pool
	}

	if p.CertFile != "" && p.KeyFile == "" {
		log.Error("Failed to create a client for Mesos, error: Missing private key")
	}

	if p.CertFile == "" && p.KeyFile != "" {
		log.Error("Failed to create a client for Mesos, error: Missing certificate")
	}

	if p.CertFile != "" && p.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(p.CertFile, p.KeyFile)
		if err != nil {
			log.Errorf("Failed to create a client for Mesos, error: %s", err)
		} else {
			p.Cert = cert
		}
	}
}

func (p *Provider) initAuthentication() {
	configMapOpts := httpclient.ConfigMapOptions{
		basic.Configuration(p.Credentials),
	}
	if p.IAMConfigFile != "" {
		iamConfig, err := iam.LoadFromFile(p.IAMConfigFile)
		if err != nil {
			log.Errorf("Failed to create a client for Mesos, error: %s", err)
		}
		configMapOpts = append(configMapOpts, iam.Configuration(iamConfig))
	}

	p.HTTPConfigMap = configMapOpts.ToConfigMap()
	err := httpclient.Validate(p.Authentication, p.HTTPConfigMap)
	if err != nil {
		log.Errorf("Failed to create a client for Mesos, error: %s", err)
	}
}

func readCACertFile(caCertFile string) (caPool *x509.CertPool, err error) {
	var f *os.File
	if f, err = os.Open(caCertFile); err != nil {
		err = fmt.Errorf("CACertFile open failed: %v", err)
		return
	}
	defer httpclient.Ignore(f.Close)

	var b []byte
	if b, err = ioutil.ReadAll(f); err != nil {
		err = fmt.Errorf("CACertFile read failed: %v", err)
		return
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		err = fmt.Errorf("CACertFile parsing failed: %v", err)
	} else {
		caPool = pool
	}
	return
}
