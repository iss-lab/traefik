package metrics

import (
	"bytes"
	"fmt"
	"time"

	"github.com/containous/traefik/log"
	"github.com/containous/traefik/safe"
	"github.com/containous/traefik/types"
	kitlog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics/influx"
	influxdb "github.com/influxdata/influxdb/client/v2"
)

var influxDBClient *influx.Influx

type influxDBWriter struct {
	dbCreated bool
	buf       bytes.Buffer
	config    *types.InfluxDB
}

var influxDBTicker *time.Ticker

const (
	influxDBMetricsBackendReqsName      = "traefik.backend.requests.total"
	influxDBMetricsBackendLatencyName   = "traefik.backend.request.duration"
	influxDBRetriesTotalName            = "traefik.backend.retries.total"
	influxDBConfigReloadsName           = "traefik.config.reload.total"
	influxDBConfigReloadsFailureName    = influxDBConfigReloadsName + ".failure"
	influxDBLastConfigReloadSuccessName = "traefik.config.reload.lastSuccessTimestamp"
	influxDBLastConfigReloadFailureName = "traefik.config.reload.lastFailureTimestamp"
	influxDBEntrypointReqsName          = "traefik.entrypoint.requests.total"
	influxDBEntrypointReqDurationName   = "traefik.entrypoint.request.duration"
	influxDBEntrypointOpenConnsName     = "traefik.entrypoint.connections.open"
	influxDBOpenConnsName               = "traefik.backend.connections.open"
	influxDBServerUpName                = "traefik.backend.server.up"
)

// RegisterInfluxDB registers the metrics pusher if this didn't happen yet and creates a InfluxDB Registry instance.
func RegisterInfluxDB(config *types.InfluxDB) Registry {
	if influxDBClient == nil {
		log.Debugf("Creating influxDB client with config: %s, %s, %s, %s", config.Address, config.PushInterval, config.Database, config.RetentionPolicy)
		influxDBClient = influx.New(
			map[string]string{},
			influxdb.BatchPointsConfig{
				Database:        config.Database,
				RetentionPolicy: config.RetentionPolicy,
			},
			kitlog.LoggerFunc(func(keyvals ...interface{}) error {
				log.Info(keyvals)
				return nil
			}))
	}
	if influxDBTicker == nil {
		influxDBTicker = initInfluxDBTicker(config)
	}

	return &standardRegistry{
		enabled:                        true,
		configReloadsCounter:           influxDBClient.NewCounter(influxDBConfigReloadsName),
		configReloadsFailureCounter:    influxDBClient.NewCounter(influxDBConfigReloadsFailureName),
		lastConfigReloadSuccessGauge:   influxDBClient.NewGauge(influxDBLastConfigReloadSuccessName),
		lastConfigReloadFailureGauge:   influxDBClient.NewGauge(influxDBLastConfigReloadFailureName),
		entrypointReqsCounter:          influxDBClient.NewCounter(influxDBEntrypointReqsName),
		entrypointReqDurationHistogram: influxDBClient.NewHistogram(influxDBEntrypointReqDurationName),
		entrypointOpenConnsGauge:       influxDBClient.NewGauge(influxDBEntrypointOpenConnsName),
		backendReqsCounter:             influxDBClient.NewCounter(influxDBMetricsBackendReqsName),
		backendReqDurationHistogram:    influxDBClient.NewHistogram(influxDBMetricsBackendLatencyName),
		backendRetriesCounter:          influxDBClient.NewCounter(influxDBRetriesTotalName),
		backendOpenConnsGauge:          influxDBClient.NewGauge(influxDBOpenConnsName),
		backendServerUpGauge:           influxDBClient.NewGauge(influxDBServerUpName),
	}
}

// initInfluxDBTicker initializes metrics pusher and creates a influxDBClient if not created already
func initInfluxDBTicker(config *types.InfluxDB) *time.Ticker {
	pushInterval, err := time.ParseDuration(config.PushInterval)
	if err != nil {
		log.Warnf("Unable to parse %s into pushInterval, using 10s as default value", config.PushInterval)
		pushInterval = 10 * time.Second
	}

	report := time.NewTicker(pushInterval)

	safe.Go(func() {
		var buf bytes.Buffer
		influxDBClient.WriteLoop(report.C, &influxDBWriter{dbCreated: false, buf: buf, config: config})
	})

	return report
}

// StopInfluxDB stops internal influxDBTicker which controls the pushing of metrics to InfluxDB Agent and resets it to `nil`
func StopInfluxDB() {
	if influxDBTicker != nil {
		influxDBTicker.Stop()
	}
	influxDBTicker = nil
}

func (w *influxDBWriter) Write(bp influxdb.BatchPoints) (err error) {
	var c influxdb.Client
	if w.config.HTTPAddress != "" {
		c, err = influxdb.NewHTTPClient(influxdb.HTTPConfig{
			Addr: w.config.HTTPAddress,
		})
	} else {
		c, err = influxdb.NewUDPClient(influxdb.UDPConfig{
			Addr: w.config.Address,
		})
	}

	if err != nil {
		return
	}

	defer c.Close()

	if !w.dbCreated && w.config.HTTPAddress != "" {
		w.createInfluxDBDatabase(c)
	}

	return c.Write(bp)
}

// createInfluxDBDatabase attempts to create a if it hasn't yet
func (w *influxDBWriter) createInfluxDBDatabase(c influxdb.Client) {
	if w.config.Database != "" {
		log.Debugf("Creating influxDB database / RP: %s, %s", w.config.Database, w.config.RetentionPolicy)
		qStr := fmt.Sprintf("CREATE DATABASE %s", w.config.Database)
		if w.config.RetentionPolicy != "" {
			qStr = fmt.Sprintf("%s WITH NAME %s", qStr, w.config.RetentionPolicy)
		}
		log.Debugf("InfluxDB create database query: %s", qStr)
		q := influxdb.NewQuery(qStr, "", "")
		response, queryErr := c.Query(q)
		if queryErr != nil {
			log.Errorf("Error creating InfluxDB database: %s", queryErr)
			return
		}
		if response.Error() != nil {
			log.Errorf("Error creating InfluxDB database: %s", response.Error())
			return
		}
		w.dbCreated = true
		log.Debugf("Create db results: %s", response.Results)
	}
}
