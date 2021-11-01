/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	log "github.com/ethereum/go-ethereum/log"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxdb2_api "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/spf13/cobra"
)

// ETLCmd represents the ETL command
var ETLCmd = &cobra.Command{
	Use:   "ETL",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(true)))
		glogger.Verbosity(log.Lvl(flagAppVerbosity))
		log.Root().SetHandler(glogger)


		// Set up a shared instance of this client API.
		c := influxdb2.NewClient(flagInfluxEndpoint, flagInfluxToken)
		api := c.WriteAPIBlocking(flagInfluxOrg, flagInfluxBucket)

		manWU := &managerWU{
			apiKey: flagWUAPIKey,
		}

		manIF := &managerInflux{
			clientAPI: api,
			namespace: "wu.",
		}

		rc := &runConfig{
			manWU:         manWU,
			managerInflux: manIF,
			stations:      flagWUStations,
			interval:      flagAppInterval,
		}

		run(rc)
	},
}

var flagInfluxEndpoint string
var flagInfluxToken string
var flagInfluxOrg string
var flagInfluxBucket string

var flagWUStations []string
var flagWUAPIKey string

var flagAppInterval time.Duration
var flagAppVerbosity int

func init() {
	rootCmd.AddCommand(ETLCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	ETLCmd.PersistentFlags().String("foo", "", "A help for foo")

	ETLCmd.PersistentFlags().StringVar(&flagInfluxEndpoint, "influx.endpoint", "", "")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxToken, "influx.token", "", "")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxOrg, "influx.org", "", "")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxBucket, "influx.bucket", "weather/autogen", "Use slashed-delim db/retention for v1.8. Otherwise v2.")

	ETLCmd.PersistentFlags().StringSliceVar(&flagWUStations, "wu.stations", nil, "")
	ETLCmd.PersistentFlags().StringVar(&flagWUAPIKey, "wu.apikey", "", "")

	ETLCmd.PersistentFlags().DurationVar(&flagAppInterval, "app.interval", 32*time.Second, "0=oneshot")
	ETLCmd.PersistentFlags().IntVar(&flagAppVerbosity, "app.verbosity", int(log.LvlInfo), "[0..5]")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	ETLCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

type runConfig struct {
	manWU         *managerWU
	managerInflux *managerInflux
	stations      []string
	interval      time.Duration
}

type managerWU struct {
	apiKey string
}

type managerInflux struct {
	currentStation string
	clientAPI      influxdb2_api.WriteAPIBlocking
	namespace      string
}

type weatherUndergroundObservations struct {
	Observations []interface{} `json:"observations"`
}

func run(rc *runConfig) {
	rerun := true

	// This is a weird wrapper loop thing.
	// The intention is to always run the program once (and ONLY once if
	// the interval=0), but to run the program at interval N forever
	// if indeed a nonzero interval is configured.
	for rerun {

		rerun = rc.interval > 0
		interval := rc.interval

	stationsLoop:
		for _, station := range rc.stations {
			rc.managerInflux.currentStation = station

			res, err := rc.manWU.requestCurrent(station)
			if err != nil {
				interval = time.Hour // If we encounter an error reading from the API, give it an hour. It could be a rate limit thing.
				break stationsLoop
			}
			for i, obs := range res.Observations {
				start := time.Now()
				err := rc.managerInflux.record(obs)
				if err != nil {
					log.Error("Post InfluxDB", "error", err)
					continue
				}
				log.Info("Posted observation to influx", "i", i, "elapsed", time.Since(start).Round(time.Millisecond))
			}
		}
		if rerun {
			log.Warn("Sleeping", "interval", interval)
			time.Sleep(rc.interval)
		}
	}
}

func (m *managerWU) requestCurrent(station string) (res *weatherUndergroundObservations, err error) {
	payload := url.Values{}
	payload.Add("stationId", station)
	payload.Add("format", "json")
	payload.Add("units", "m")
	payload.Add("apiKey", m.apiKey)
	endpoint := "https://api.weather.com/v2/pws/observations/current?" + payload.Encode()
	requestLogger := log.New("HTTP.GET", endpoint)

	requestStart := time.Now()
	response, err := http.Get(endpoint)
	if err != nil {
		requestLogger.Error("Request weatherunderground API", "error", err)
		return nil, err
	}
	if response.StatusCode >= 300 || response.StatusCode < 200 {
		requestLogger.Error("Request weatherunderground bad response", "res.code", response.StatusCode, "res", response.Status)
		return nil, fmt.Errorf("request failed: %d", response.StatusCode)
	}

	// Request has been made OK.
	requestLogger.Info("OK", "elapsed", time.Since(requestStart).Round(time.Millisecond))

	// Decode the response body.
	data := &weatherUndergroundObservations{}
	dataBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(dataBytes, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *managerInflux) record(obs interface{}) error {
	obsT, ok := obs.(map[string]interface{})
	if !ok {
		return errors.New("failed to cast observation to map")
	}
	m.postMapRecursive("", obsT)
	return nil
}

func (m *managerInflux) postMapRecursive(parentAnnotation string, myMap map[string]interface{}) {
	now := time.Now()
	ctx := context.Background()
	if parentAnnotation != "" {
		parentAnnotation = parentAnnotation + "."
	}
mapLoop:
	for k, v := range myMap {

		// Recurse and break if the type is a map "ie 'metric'/'imperial' or whatever
		switch t := v.(type) {
		case map[string]interface{}:
			m.postMapRecursive(k, t)
			continue mapLoop
		}

		measurement := fmt.Sprintf("%s%s%s.gauge", m.namespace, parentAnnotation, k)
		fields := map[string]interface{}{
			"value": v,
		}

		tags := map[string]string{
			"stationID": m.currentStation,
		}

		pt := influxdb2.NewPoint(measurement, tags, fields, now)

		if err := m.clientAPI.WritePoint(ctx, pt); err != nil {
			log.Error("Write point", "error", err)
		} else {
			log.Debug("Wrote point", "station", m.currentStation, measurement, v)
		}
	}
}

