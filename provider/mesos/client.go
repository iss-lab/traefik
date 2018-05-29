package mesos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/containous/traefik/log"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpmaster"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	"github.com/mesos/mesos-go/api/v1/lib/master/calls"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	nodeJSONPrefix  = "json.info_"
	operatorAPIPath = "/api/v1"
)

type Client struct {
	masterTimeout time.Duration
	masterTimer   *time.Timer
	masters       []string
	stopCh        chan bool
}

func MkClientFn(endpoint string, zkTimeout, masterTimeout int) func() (*Client, error) {
	return func() (*Client, error) {
		masters, err := getMasters(endpoint, zkTimeout)
		if err != nil {
			return nil, err
		}
		if len(masters) < 1 {
			return nil, fmt.Errorf("Unable to find initial master list")
		}
		return NewClient(masters, masterTimeout), nil
	}
}

func NewClient(masters []string, masterTimeoutInt int) *Client {
	c := &Client{
		masters:       masters,
		masterTimeout: time.Second * time.Duration(masterTimeoutInt),
		stopCh:        make(chan bool),
	}
	return c
}

func (c *Client) Watch() (<-chan master.Event, <-chan error) {
	eventCh := make(chan master.Event, 1)
	errCh := make(chan error)

	go func() {
		c.masterTimer = time.AfterFunc(c.masterTimeout, func() {
			if c.masterTimeout > 0 {
				errCh <- fmt.Errorf("master detection timed out after %s", c.masterTimeout)
			}
		})
		resp, err := c.getStream()
		defer func() {
			if resp != nil {
				resp.Close()
			}
		}()
		for err == nil {
			select {
			case <-c.stopCh:
				c.masterTimer.Stop()
				return
			default:
				var e master.Event
				if err = resp.Decode(&e); err == nil {
					if e.GetType() == master.Event_HEARTBEAT {
						c.masterTimer.Reset(c.masterTimeout)
					}
					eventCh <- e
				}
			}
		}
		if err != nil && err != io.EOF {
			errCh <- err
		}
	}()

	return eventCh, errCh
}

func (c *Client) Stop() {
	c.stopCh <- true
}

func (c *Client) getStream() (mesos.Response, error) {
	var resp mesos.Response
	var err error

	ctx := context.Background()

	for i := 0; i < len(c.masters); i++ {
		httpOpts := []httpcli.Opt{
			httpcli.Endpoint(c.masters[i]),
		}
		cli := httpmaster.NewSender(httpcli.New(httpOpts...).Send)
		resp, err = cli.Send(ctx, calls.NonStreaming(calls.Subscribe()))
		if err == nil {
			break
		}
	}
	return resp, err
}

func getMasters(endpoint string, timeout int) (masters []string, err error) {
	rawMasters := []string{}
	masters = []string{}
	if strings.HasPrefix(endpoint, "zk://") {
		rawMasters, err = getZKMasters(endpoint, timeout)
		if err != nil {
			return masters, err
		}
	} else {
		rawMasters = strings.Split(endpoint, ",")
	}
	for _, m := range rawMasters {
		if u, err := url.Parse(m); err == nil {
			if u.Scheme != "http" && u.Scheme != "https" {
				log.Debugf("Mesos address %s did not specify a scheme, defaulting to http.", m)
				masters = append(masters, "http://"+m+operatorAPIPath)
			} else {
				masters = append(masters, m+operatorAPIPath)
			}
		} else {
			log.Warnf("Unable to parse mesos address %s: %v, attempting to parse as ip.", m, err)
			if h, _, err := net.SplitHostPort(m); err == nil {
				if ip := net.ParseIP(h); ip == nil {
					log.Warnf("Unable to Parse mesos address %s as url or ip address", m)
				}
			} else {
				log.Warnf("Unable to Parse mesos address %s as url or host:port, error: %v", m, err)
			}
			masters = append(masters, "http://"+m+operatorAPIPath)
		}
	}
	log.Debugf("Found Masters: %s", masters)
	return masters, nil
}

func getZKMasters(zkURLs string, timeout int) ([]string, error) {
	masters := []string{}
	endpoints, path, err := parseZK(zkURLs)
	zkTimeout := time.Second * time.Duration(timeout)
	if err != nil {
		return masters, err
	}
	conn, _, err := zk.Connect(endpoints, zkTimeout)
	if err != nil {
		return masters, err
	}
	children, _, err := conn.Children(path)
	if err != nil {
		return masters, err
	}
	for _, node := range children {
		if strings.HasPrefix(node, nodeJSONPrefix) {
			data, _, err := conn.Get(fmt.Sprintf("%s/%s", path, node))
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve leader data: %v", err)
			}
			masterInfo := &mesos.MasterInfo{}
			err = json.Unmarshal(data, masterInfo)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal json MasterInfo data from zookeeper: %v", err)
			}
			address := masterInfo.GetAddress()
			host := address.GetIP()
			port := address.GetPort()
			hostPort := net.JoinHostPort(host, strconv.Itoa(int(port)))
			masters = append(masters, hostPort)
		} else {
			continue
		}
	}
	return masters, nil
}

func parseZK(zkURLs string) ([]string, string, error) {
	u, err := url.Parse(zkURLs)
	if err != nil {
		log.Infof("failed to parse url: %v", err)
		return nil, "", err
	}
	if u.Scheme != "zk" {
		return nil, "", fmt.Errorf("invalid url scheme for zk url: '%v'", u.Scheme)
	}
	return strings.Split(u.Host, ","), u.Path, nil
}
