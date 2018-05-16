package state

import (
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/containous/traefik/log"
	"github.com/containous/traefik/provider/mesos/httpclient"
	"github.com/containous/traefik/provider/mesos/httpclient/urls"
)

type (
	// StateLoader attempts to read state from the leading Mesos master and return the parsed content.
	StateLoader func(masters []string) (State, error)

	// Unmarshaler parses raw byte content into a State object
	Unmarshaler func([]byte, *State) error
)

// NewStateLoader generates a new Mesos master state loader using the given http client and initial endpoint.
func NewStateLoader(doer httpclient.Doer, initialEndpoint urls.Builder, unmarshal Unmarshaler) StateLoader {
	return func(masters []string) (State, error) {
		return LoadMasterStateTryAll(masters, func(ip, port string) (State, error) {
			return LoadMasterStateFailover(ip, func(tryIP string) (State, error) {
				return LoadMasterState(doer, initialEndpoint, tryIP, port, unmarshal)
			})
		})
	}

}

// LoadMasterStateTryAll tries each master and looks for the leader; if no leader responds it errors.
// The first master in the list is assumed to be the leading mesos master.
func LoadMasterStateTryAll(masters []string, stateLoader func(ip, port string) (State, error)) (State, error) {
	var sj State
	var leader string

	if len(masters) > 0 {
		leader, masters = masters[0], masters[1:]
	}

	// Check if ZK leader is correct
	if leader != "" {
		log.Debugf("Zookeeper says the leader is: %s", leader)
		ip, port, err := urls.SplitHostPort(leader)
		if err != nil {
			log.Errorf("%s", err)
		} else {
			if sj, err = stateLoader(ip, port); err == nil {
				return sj, nil
			}
			log.Errorf("%s", "Failed to fetch json from leader. Error: ", err)
			if len(masters) == 0 {
				log.Errorf("%s", "No more masters to try, returning last error")
				return sj, err
			}
			log.Errorf("%s", "Falling back to remaining masters: ", masters)
		}
	}

	// try each listed mesos master before dying
	var (
		ip, port string
		err      error
	)
	for _, master := range masters {
		ip, port, err = urls.SplitHostPort(master)
		if err != nil {
			log.Errorf("%s", err)
			continue
		}

		if sj, err = stateLoader(ip, port); err != nil {
			log.Errorf("%s", "Failed to fetch json - trying next one. Error: ", err)
			continue
		}
		return sj, nil
	}

	log.Errorf("%s", "No more masters eligible for json query, returning last error")
	return sj, err
}

// LoadMasterStateFailover catches an attempt to load json from a mesos master.
// Attempts can fail from due to a down server or if contacting a mesos master secondary.
// It reloads from a different master if the contacted master is a secondary.
func LoadMasterStateFailover(initialMasterIP string, stateLoader func(ip string) (State, error)) (State, error) {
	var err error
	var sj State

	log.Debugf("reloading from master %s", initialMasterIP)
	sj, err = stateLoader(initialMasterIP)
	if err != nil {
		return State{}, err
	}
	if sj.Leader != "" {
		var stateLeaderIP string

		stateLeaderIP, err = leaderIP(sj.Leader)
		if err != nil {
			return sj, err
		}
		if stateLeaderIP != initialMasterIP {
			log.Debugf("Warning: master changed to %s", stateLeaderIP)
			return stateLoader(stateLeaderIP)
		}
		return sj, nil
	}
	err = errors.New("Fetched json does not contain leader information")
	return sj, err
}

// LoadMasterState loads json from mesos master
func LoadMasterState(client httpclient.Doer, stateEndpoint urls.Builder, ip, port string, unmarshal Unmarshaler) (sj State, _ error) {
	// REFACTOR: json security

	u := url.URL(stateEndpoint.With(urls.Host(net.JoinHostPort(ip, port))))

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Errorf("%s", err)
		return State{}, err
	}

	req.Header.Set("Content-Type", "application/json") // TODO(jdef) unclear why Content-Type vs. Accept
	req.Header.Set("User-Agent", "Mesos-DNS")

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("%s", err)
		return sj, err
	}

	defer httpclient.Ignore(resp.Body.Close)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("%s", err)
		return sj, err
	}

	err = unmarshal(body, &sj)
	if err != nil {
		log.Errorf("%s", err)
		return sj, err
	}

	return
}

// leaderIP returns the ip for the mesos master
// input format master@ip:port
func leaderIP(leader string) (string, error) {
	// TODO(jdef) it's unclear why we drop the port here
	nameAddressPair := strings.Split(leader, "@")
	if len(nameAddressPair) != 2 {
		return "", errors.New("Invalid leader address: " + leader)
	}
	hostPort := nameAddressPair[1]
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", err
	}
	return host, nil
}
