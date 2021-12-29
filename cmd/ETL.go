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
	"path/filepath"
	"strings"
	"time"

	log "github.com/ethereum/go-ethereum/log"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxdb2_api "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/spf13/cobra"
	"github.com/tidwall/gjson"
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
var flagAppDatadir string

var flagAppForecastEnable bool
var flagAppForecastGeocode string
var forecastExpiry time.Time

func init() {
	rootCmd.AddCommand(ETLCmd)

	// Here you will define your flags and configuration settings.

	ETLCmd.PersistentFlags().StringVar(&flagInfluxEndpoint, "influx_endpoint", "", "")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxToken, "influx_token", "", "user:pass for v1.8")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxOrg, "influx_org", "", "")
	ETLCmd.PersistentFlags().StringVar(&flagInfluxBucket, "influx_bucket", "weather/autogen", "Use slashed-delim db/retention for v1.8. Otherwise v2.")

	ETLCmd.PersistentFlags().StringSliceVar(&flagWUStations, "wu_stations", nil, "")
	ETLCmd.PersistentFlags().StringVar(&flagWUAPIKey, "wu_apikey", "", "")

	ETLCmd.PersistentFlags().DurationVar(&flagAppInterval, "app_interval", 32*time.Second, "0=oneshot")
	ETLCmd.PersistentFlags().IntVar(&flagAppVerbosity, "app_verbosity", int(log.LvlInfo), "[0..5]")
	ETLCmd.PersistentFlags().StringVar(&flagAppDatadir, "app_datadir", filepath.Join("/var", "lib", "wunderground-influxdb"), "Data directory for persistent storage")

	ETLCmd.PersistentFlags().BoolVar(&flagAppForecastEnable, "app_forecast", false, "Enable forecasting metrics")
	ETLCmd.PersistentFlags().StringVar(&flagAppForecastGeocode, "app_forecast_geocode", "48.029,-118.367", "Data directory for persistent storage")
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

			var res *weatherUndergroundObservations
			var err error
			try := 0
			for try < 3 && ((try == 0) || (err != nil && (strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "reset")))) {
				res, err = rc.manWU.requestCurrent(station)
				try++
			}
			if err != nil {
				if strings.Contains(err.Error(), "request failed") {
					interval = time.Hour // If we encounter an error reading from the API, give it an hour. It could be a rate limit thing.
				} else {
					// We assume that the error was a network/network-unreachable error.
					// This is not our fault (its the internet's fault); not bad API usage, and we should not bump the interval.
				}
				break stationsLoop
			}
			for i, obs := range res.Observations {
				start := time.Now()
				err := rc.managerInflux.recordObs(obs)
				if err != nil {
					log.Error("Post InfluxDB observation", "error", err)
					continue
				}
				log.Info("Posted observation to influx", "i", i, "elapsed", time.Since(start).Round(time.Millisecond))
			}
		}

		if flagAppForecastEnable {
			if time.Now().After(forecastExpiry) {

				// Function will set forecastExpiry to the first value given in the forecast data
				res, err := rc.manWU.requestForecast()
				if err != nil {
					log.Error("Forecast request failed", "error", err)
				} else {
					// Forecast was received OK.
					err = rc.managerInflux.recordForecast(res)
					if err != nil {
						log.Error("Post InfluxDB forecast", "error", err)
					} else {
						// Forecast has been recorded OK.
						//
						// Now get the expiry time from the forecast data,
						// and update our global value.
						jval := gjson.GetBytes(res, "expirationTimeUtc.0")
						forecastExpiry = time.Unix(jval.Int(), 0)
						log.Info("Forecast expiry reset", "expiry", forecastExpiry.Round(time.Second), "expiry.from_now", forecastExpiry.Sub(time.Now()).Round(time.Second))
					}
				}
			}
		}

		if rerun {
			log.Warn("Sleeping", "interval", interval)
			time.Sleep(interval)
		}
	}
}

type precipStore struct {
	Latest     float64 `json:"latest"`
	Cumulative float64 `json:"cumulative"`
}

func getMetricPrecipRealTotal(annotation string) *precipStore {
	os.MkdirAll(flagAppDatadir, os.ModePerm)
	data, err := ioutil.ReadFile(filepath.Join(flagAppDatadir, annotation+"precipitation"))
	if err != nil {
		return nil
	}
	v := &precipStore{}
	err = json.Unmarshal(data, v)
	if err != nil {
		return nil
	}
	return v
}

func saveMetricPrecipRealTotal(annotation string, store *precipStore) {
	os.MkdirAll(flagAppDatadir, os.ModePerm)
	data, err := json.Marshal(store)
	if err != nil {
		log.Error("Failed to marshal JSON precip store", "error", err)
		return
	}
	err = ioutil.WriteFile(filepath.Join(flagAppDatadir, annotation+"precipitation"), data, os.ModePerm)
	if err != nil {
		panic(err)
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
	http.DefaultClient.Timeout = time.Second * 30
	response, err := http.Get(endpoint)
	if err != nil {
		requestLogger.Error("Request weatherunderground API", "error", err, "elapsed", time.Since(requestStart).Round(time.Millisecond))
		return nil, err
	}
	if response.StatusCode >= 300 || response.StatusCode < 200 {
		requestLogger.Error("Request weatherunderground bad response", "res.code", response.StatusCode, "res", response.Status,
			"elapsed", time.Since(requestStart).Round(time.Millisecond))
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

func (m *managerWU) requestForecast() (res []byte, err error) {
	payload := url.Values{}
	payload.Add("geocode", flagAppForecastGeocode)
	payload.Add("format", "json")
	payload.Add("units", "m")
	payload.Add("language", "en-US")
	payload.Add("apiKey", m.apiKey)
	endpoint := "https://api.weather.com/v3/wx/forecast/daily/5day?" + payload.Encode()
	requestLogger := log.New("HTTP.GET", endpoint)

	requestStart := time.Now()
	http.DefaultClient.Timeout = time.Second * 30
	response, err := http.Get(endpoint)
	if err != nil {
		requestLogger.Error("Request weatherunderground API", "error", err, "elapsed", time.Since(requestStart).Round(time.Millisecond))
		return nil, err
	}
	if response.StatusCode >= 300 || response.StatusCode < 200 {
		requestLogger.Error("Request weatherunderground bad response", "res.code", response.StatusCode, "res", response.Status,
			"elapsed", time.Since(requestStart).Round(time.Millisecond))
		return nil, fmt.Errorf("request failed: %d", response.StatusCode)
	}

	// Request has been made OK.
	requestLogger.Info("OK", "elapsed", time.Since(requestStart).Round(time.Millisecond))

	// Decode the response body.
	dataBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return dataBytes, nil
}

func (m *managerInflux) recordObs(obs interface{}) error {
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

	precipTotal, ok := myMap["precipTotal"]
	if ok {
		precipTotalCurrentValue := precipTotal.(float64)
		store := getMetricPrecipRealTotal(parentAnnotation)
		if store == nil {
			// Initialize store
			store = &precipStore{
				Latest:     precipTotalCurrentValue,
				Cumulative: precipTotalCurrentValue,
			}
		} else {
			if precipTotalCurrentValue < store.Latest {
				// We have rolled over (eg midnight reset to 0).
				// Increment Cumulative value by the prior-latest.
				store.Cumulative += store.Latest
			}
		}
		store.Latest = precipTotalCurrentValue
		saveMetricPrecipRealTotal(parentAnnotation, store)

		myMap["precipCumulative"] = store.Cumulative + store.Latest
	}

mapLoop:
	for k, v := range myMap {

		// Recurse and break if the type is a map "ie 'metric'/'imperial' or whatever
		switch t := v.(type) {
		case map[string]interface{}:
			m.postMapRecursive(k, t)
			continue mapLoop
		case []interface{}:
			// Unhandled; no data.
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

var forecastFields = []string{
	"calendarDayTemperatureMax",
	"calendarDayTemperatureMin",
	"moonPhaseDay",
	"moonPhase",
	"narrative",
	"qpf",
	"qpfSnow",
	"temperatureMax",
	"temperatureMin",
	"daypart.0.narrative",
	"daypart.0.cloudCover",
	"daypart.0.dayOrNight",
	"daypart.0.daypartName",
	"daypart.0.precipChance",
	"daypart.0.precipType",
	"daypart.0.qpf",
	"daypart.0.qpfSnow",
	"daypart.0.qualifierPhrase",
	"daypart.0.relativeHumidity",
	"daypart.0.temperature",
	"daypart.0.temperatureHeatIndex",
	"daypart.0.temperatureWindChill",
	"daypart.0.thunderCategory",
	"daypart.0.thunderIndex",
	"daypart.0.uvDescription",
	"daypart.0.uvIndex",
	"daypart.0.windDirection",
	"daypart.0.windDirectionCardinal",
	"daypart.0.windSpeed",
	"daypart.0.wxPhraseLong",
}

func (m *managerInflux) recordForecast(forecastData []byte) error {
	now := time.Now()
	ctx := context.Background()

	// Assume: this field (daypart.0.narrative) is
	// the first we'll encounter of the daypart object,
	// and its first value (.0) will always be null when the response is given at night time,
	// and it will never be null in the morning.
	isNight := gjson.GetBytes(forecastData, "daypart.0.narrative.0").Type == gjson.Null

	for _, f := range forecastFields {
		res := gjson.GetBytes(forecastData, f)
		log.Debug("JSON", f, res.Value())

		// Create point using fluent style
		measurementName := fmt.Sprintf("%s/forecast/%s", m.namespace, f)
		p := influxdb2.NewPointWithMeasurement(measurementName).SetTime(now)

		resSl, ok := res.Value().([]interface{})
		if !ok {
			return fmt.Errorf("could not cast field value to slice: %s", f)
		}

		/*
		   calendarDayTemperatureMax => [15 8 5 4 5 5]
		   calendarDayTemperatureMin => [4 -2 -4 -1 -1 0]
		   moonPhaseDay => [11 12 13 14 15 16]
		   moonPhase => [Waxing Gibbous Waxing Gibbous Waxing Gibbous Full Moon Full Moon Waning Gibbous]
		*/
		for i, it := range resSl {
			// The first element can be null.
			// The API will do this when the day part of the daypart has expired;
			// then, it'll be evening (local time), and only the night part of the daypart (index=1)
			// will be relevant.
			// This is a kind of weird API.
			// So, since we're living in time series database world, setting null values
			// as the first item in the forecast is useless, and I think will be a PIA
			// to handle from the InfluxQL side.
			// So if the first element is null, we're going to pretend like it doesn't exist.
			// To do this, we need to decrement the index.
			if isNight && i == 0 {
				// Always skip the first value which will be 'null' when the forecast is given in the night.
				continue
			}
			fieldIndex := i
			if isNight {
				// At night, pretend like they didn't send us null values for the past morning.
				fieldIndex -= 1
			}
			p.AddField(fmt.Sprintf("%d", fieldIndex), it)
		}

		if err := m.clientAPI.WritePoint(ctx, p); err != nil {
			log.Error("Write point", "error", err)
			return err
		} else {
			log.Debug("Wrote point", "type", "forecast", measurementName, resSl)
		}
	}

	return nil
}
