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

var (
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
		glogger.Verbosity(log.Lvl(log.LvlDebug))
		log.Root().SetHandler(glogger)

		// ---------------------------------- SETUP/DEBUG
		// log.Debug("ETL called")
		// // log.Debug(cmd.ParseFlags(args)) // This is unnecessary, but harmless.
		// fooVal, _ := cmd.PersistentFlags().GetString("foo")
		// log.Debug("flags", "foo", fooVal)
		//
		// toggleVal, _ := cmd.Flags().GetBool("toggle")
		// log.Debug("flags", "toggle", toggleVal)
		//
		// dneVal, err := cmd.Flags().GetBool("dne")
		// log.Debug("flags", "dne", dneVal, "err", err)
		//
		// log.Info("test")

		// ---------------------------------- EO SETUP/DEBUG


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

type runConfig struct {
	manWU *managerWU
	managerInflux *managerInflux
	stations []string
	interval time.Duration
}

type managerWU struct {
	apiKey string
}

type managerInflux struct {
	currentStation string
clientAPI          influxdb2_api.WriteAPIBlocking
namespace          string
}

type weatherUndergroundObservation struct {
	// // stuff..
	// StationID string `json:"stationID"`
	//
	// ObsTimeUtc string `json:"obsTimeUtc"`
	// ObsTimeUTCTime time.Time
	// ObsTimeLocal string `json:"obsTimeLocal"`
	// ObsTimeLocalTime time.Time
	//
	// Neighborhood string `json:"neighborhood"`
	// SoftwareType *float64 `json:"softwareType"`
	// Country float64 `json:"country"`
	// SolarRadiation *float64 `json:"solarRadiation"`
	// Lon float64 `json:"lon"`
	// RealtimeFrequency *float64 `json:"realtimeFrequency"`
	// Epoch uint64 `json:"epoch"`
	// Lat float64 `json:"lat"`
	// Uv *float64 `json:"uv"`
	// Winddir int64 `json:"winddir"`
	// Humidity int64 `json:"humidity"`
	// QcStatus float64 `json:"qcStatus"`
	//
	// Metric weatherUndergroundObservationReport `json:"metric"`
	// Imperial /* ??? */ weatherUndergroundObservationReport `json:"imperial"`
}
//
// func (w *weatherUndergroundObservation) MustInflate() {
// 	var err error
// 	w.ObsTimeUTCTime, err = time.Parse(time.RFC3339, w.ObsTimeUtc)
// 	if err != nil {
// 		log.Crit("Parse observation UTC time", "error", err)
// 	}
// }

type weatherUndergroundObservationReport struct {
	// Temp int64 `json:"temp"`
	// HeatIndex int64 `json:"heatIndex"`
	// Dewpt int64 `json:"dewpt"`
	// WindChill int64 `json:"windChill"`
	// WindSpeed int64 `json:"windSpeed"`
	// WindGust int64 `json:"windGust"`
	// Pressure float64 `json:"pressure"`
	// PrecipRate float64 `json:"precipRate"`
	// PrecipTotal float64 `json:"precipTotal"`
	// Elev int64 `json:"elev"`
}

type weatherUndergroundObservations struct {
	Observations []interface{} `json:"observations"`
}

func run(rc *runConfig) {
	rerun := true

	// This is a weird wrapper loop thing.
	// The intention is to always run the program once (and ONLY once if
	// the interval=0), but to run the program at internal N forever
	// if indeed a nonzero interval is configured.
	for rerun {

		stationsLoop:
		for _, station := range rc.stations {
			rc.managerInflux.currentStation = station

			res, err := rc.manWU.requestWU(station)
			if err != nil {
				log.Error("Request weatherunderground API", "error", err)
				break stationsLoop
			}
			for _, obs := range res.Observations {
				err := rc.managerInflux.recordInfluxObservation(obs)
				if err != nil {
					log.Error("Post InfluxDB", "error", err)
					continue
				}
			}
		}
		rerun = rc.interval > 0
		if rerun {
			time.Sleep(rc.interval)
		}
	}
}

func (m *managerWU) requestWU(station string) (res *weatherUndergroundObservations, err error) {
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
		return nil, err
	}
	if response.StatusCode > 300 || response.StatusCode < 200 {
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

func (m *managerInflux) recordInfluxObservation(obs interface{}) error {
	obsT := obs.(map[string]interface{})
	m.postMapRecursive("", obsT)

	return nil
}

var flagInfluxEndpoint string
var flagInfluxToken string
var flagInfluxOrg string
var flagInfluxBucket string

var flagWUStations []string
var flagWUAPIKey string

var flagAppInterval time.Duration

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

	ETLCmd.PersistentFlags().DurationVar(&flagAppInterval, "app.interval", 32* time.Second, "0=oneshot")


	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	ETLCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
