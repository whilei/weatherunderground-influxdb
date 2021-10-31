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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	log "github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	metricsI "github.com/ethereum/go-ethereum/metrics/influxdb"
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
		log.Debug("ETL called")
		// log.Debug(cmd.ParseFlags(args)) // This is unnecessary, but harmless.
		fooVal, _ := cmd.PersistentFlags().GetString("foo")
		log.Debug("flags", "foo", fooVal)

		toggleVal, _ := cmd.Flags().GetBool("toggle")
		log.Debug("flags", "toggle", toggleVal)

		dneVal, err := cmd.Flags().GetBool("dne")
		log.Debug("flags", "dne", dneVal, "err", err)

		log.Info("test")

		// ---------------------------------- EO SETUP/DEBUG

		// InfluxDB credentials
		influxEndpointVal, _ := cmd.PersistentFlags().GetString("influx.endpoint")
		influxTokenVal, _ := cmd.PersistentFlags().GetString("influx.token")
		influxOrgVal, _ := cmd.PersistentFlags().GetString("influx.org")
		influxBucketVal, _ := cmd.PersistentFlags().GetString("influx.bucket")

		// WeatherUnderground Credentials
		wuStationsVal, _ := cmd.PersistentFlags().GetStringSlice("wu.stations")
		wuAPIKeyVal, _ := cmd.PersistentFlags().GetString("wu.apikey")

		// Application config
		appIntervalVal, _ := cmd.PersistentFlags().GetDuration("etl.interval")


		c := influxdb2.NewClient(influxEndpointVal, influxTokenVal)
		api := c.WriteAPIBlocking(influxOrgVal, influxBucketVal)

		manWU := &managerWU{apiKey: wuAPIKeyVal}

		manIF := &managerInflux{
			ifClient: api,
			namespace: "wu.",

		}

		rc := &runConfig{
			manWU:         manWU,
			managerInflux: manIF,
			stations:      wuStationsVal,
			interval:      appIntervalVal,
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
ifClient influxdb2_api.WriteAPIBlocking
namespace string
}

type weatherUndergroundObservation struct {
	// stuff..
	StationID string `json:"stationID"`

	ObsTimeUtc string `json:"obsTimeUtc"`
	ObsTimeUTCTime time.Time
	ObsTimeLocal string `json:"obsTimeLocal"`
	ObsTimeLocalTime time.Time

	Neighborhood string `json:"neighborhood"`
	SoftwareType *float64 `json:"softwareType"`
	Country float64 `json:"country"`
	SolarRadiation *float64 `json:"solarRadiation"`
	Lon float64 `json:"lon"`
	RealtimeFrequency *float64 `json:"realtimeFrequency"`
	Epoch uint64 `json:"epoch"`
	Lat float64 `json:"lat"`
	Uv *float64 `json:"uv"`
	Winddir int64 `json:"winddir"`
	Humidity int64 `json:"humidity"`
	QcStatus float64 `json:"qcStatus"`

	Metric weatherUndergroundObservationReport `json:"metric"`
	Imperial /* ??? */ weatherUndergroundObservationReport `json:"imperial"`
}

func (w *weatherUndergroundObservation) MustInflate() {
	var err error
	w.ObsTimeUTCTime, err = time.Parse(time.RFC3339, w.ObsTimeUtc)
	if err != nil {
		log.Crit("Parse observation UTC time", "error", err)
	}
}

type weatherUndergroundObservationReport struct {
	Temp int64 `json:"temp"`
	HeatIndex int64 `json:"heatIndex"`
	Dewpt int64 `json:"dewpt"`
	WindChill int64 `json:"windChill"`
	WindSpeed int64 `json:"windSpeed"`
	WindGust int64 `json:"windGust"`
	Pressure float64 `json:"pressure"`
	PrecipRate float64 `json:"precipRate"`
	PrecipTotal float64 `json:"precipTotal"`
	Elev int64 `json:"elev"`
}

type weatherUndergroundObservations struct {
	Observations []*weatherUndergroundObservation `json:"observations"`
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

			res, err := rc.manWU.requestWU(station)
			if err != nil {
				log.Error("Request weatherunderground API", "error", err)
				break stationsLoop
			}
			for _, obs := range res.Observations {
				obs.MustInflate()
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
	payload.Add("stationID", station)
	payload.Add("format", "json")
	payload.Add("units", "m")
	payload.Add("apiKey", m.apiKey)
	endpoint := "https://api.weather.com/v2/pws/observations/current?" + payload.Encode()
	requestLogger := log.New("HTTP.GET", endpoint)

	requestStart := time.Now()
	response, err := http.Get(endpoint)
	if err != nil {
		requestLogger.Error("", "error", err)
		return nil, err
	}
	if response.StatusCode > 300 || response.StatusCode < 200 {
		requestLogger.Error("", "statusCode", response.StatusCode, "status", response.Status)
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

var gaugeWinddir = metrics.NewRegisteredGauge("winddir", myRegistry)
var gaugeHumidity = metrics.NewRegisteredGauge("humidity", myRegistry)
var gaugeMetricTemp = metrics.NewRegisteredGauge("temp.c", myRegistry)
var gaugeMetricHI = metrics.NewRegisteredGauge("heat_index.c", myRegistry)
var gaugeMetricDewpoint = metrics.NewRegisteredGauge("dew_point", myRegistry)
var gaugeMetricWindChill = metrics.NewRegisteredGauge("wind_chill", myRegistry)
var gaugeMetricWindSpeed = metrics.NewRegisteredGauge("wind_speed", myRegistry)
var gaugeMetricWindGust = metrics.NewRegisteredGauge("wind_gust", myRegistry)
var gaugeMetricElev = metrics.NewRegisteredGauge("elev", myRegistry)

var gaugeMetricPressure = metrics.NewGaugeFloat64()
var gaugeMetricPrecipRate = metrics.NewGaugeFloat64()
var gaugeMetricPrecipTotal = metrics.NewGaugeFloat64()

var dictionaryDataMap = map[string /* */ ]interface{}

func (m *managerInflux) recordInfluxObservation(obs *weatherUndergroundObservation) error {

	values := []int64{
		obs.Winddir,
		obs.Humidity,
		obs.Metric.Temp,
		obs.Metric.HeatIndex,
		obs.Metric.Dewpt,
		obs.Metric.WindChill,
		obs.Metric.WindSpeed,
		obs.Metric.WindGust,
		obs.Metric.Elev,
	}
	// int64s
	gaugeWinddir.Update(obs.Winddir)
	gaugeHumidity.Update(obs.Humidity)
	gaugeMetricTemp.Update(obs.Metric.Temp)
	gaugeMetricHI.Update(obs.Metric.HeatIndex)
	gaugeMetricDewpoint.Update(obs.Metric.Dewpt)
	gaugeMetricWindChill.Update(obs.Metric.WindChill)
	gaugeMetricWindSpeed.Update(obs.Metric.WindSpeed)
	gaugeMetricWindGust.Update(obs.Metric.WindGust)
	gaugeMetricElev.Update(obs.Metric.Elev)

	// float64s
	gaugeMetricPressure.Update(obs.Metric.Pressure)
	gaugeMetricPrecipRate.Update(obs.Metric.PrecipRate)
	gaugeMetricPrecipTotal.Update(obs.Metric.PrecipTotal)



	return nil
}

func init() {
	if err := myRegistry.Register("pressure", gaugeMetricPrecipRate); err != nil {
		panic(err)
	}
	if err := myRegistry.Register("precip_rate", gaugeMetricPrecipRate); err != nil {
		panic(err)
	}
	if err := myRegistry.Register("precip_total", gaugeMetricPrecipTotal); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(ETLCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	ETLCmd.PersistentFlags().String("foo", "", "A help for foo")
	ETLCmd.PersistentFlags().StringArray("stations", nil, "List of PWS Station IDs")
	ETLCmd.PersistentFlags().StringArray("influx.server", nil, "List of PWS Station IDs")
	ETLCmd.PersistentFlags().StringArray("influx.user", nil, "List of PWS Station IDs")
	ETLCmd.PersistentFlags().StringArray("influx.pass", nil, "List of PWS Station IDs")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	ETLCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
