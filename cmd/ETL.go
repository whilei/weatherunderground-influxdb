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
	"net/url"
	"time"

	"net/http"
	"github.com/spf13/cobra"
	log "github.com/ethereum/go-ethereum/log"
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
		// ---------------------------------- SETUP/DEBUG
		log.Debug("ETL called")
		// log.Debug(cmd.ParseFlags(args)) // This is unnecessary, but harmless.
		fooVal, _ := cmd.PersistentFlags().GetString("foo")
		log.Debug("flags", "foo", fooVal)

		toggleVal, _ := cmd.Flags().GetBool("toggle")
		log.Debug("flags", "toggle", toggleVal)

		log.Info("test")
		// ---------------------------------- EO SETUP/DEBUG



	},
}

type managerWU struct {
	apiKey string
}

type managerInflux struct {
	dbName string
	user string
	password string
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
	Winddir float64 `json:"winddir"`
	Humidity float64 `json:"humidity"`
	QcStatus float64 `json:"qcStatus"`

	Metric weatherUndergroundObservationReport `json:"metric"`
	Imperial /* ??? */ weatherUndergroundObservationReport `json:"imperial"`
}

func (w *weatherUndergroundObservation) MustParse() {
	var err error
	w.ObsTimeUTCTime, err = time.Parse(time.RFC3339, w.ObsTimeUtc)
	if err != nil {
		log.Crit("Parse observation UTC time", "error", err)
	}
}

type weatherUndergroundObservationReport struct {
	Temp float64 `json:"temp"`
	HeatIndex float64 `json:"heatIndex"`
	Dewpt float64 `json:"dewpt"`
	WindChill float64 `json:"windChill"`
	WindSpeed float64 `json:"windSpeed"`
	WindGust float64 `json:"windGust"`
	Pressure float64 `json:"pressure"`
	PrecipRate float64 `json:"precipRate"`
	PrecipTotal float64 `json:"precipTotal"`
	Elev float64 `json:"elev"`
}

type weatherUndergroundObservations struct {
	Observations []weatherUndergroundObservation `json:"observations"`
}

func run() {
	var interval int32
	rerun := true
	stations := []string{}

	manWU := new(managerWU)
	manIF := new(managerInflux)

	for rerun {
		stationLoop:
		for _, station := range stations {

			res, err := manWU.requestWU(station)
			if err != nil {
				log.Error("Request weatherunderground API", "error", err)
				break stationLoop
			}
			for j, obs := range res.Observations {
				err := manIF.postInfluxObservation(obs)
				if err != nil {
					log.Error("Post InfluxDB", "error", err)
					continue
				}
			}
		}
		rerun = interval > 0
		if rerun {
			time.Sleep(time.Duration(interval) * time.Second)
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

func (m *managerInflux) postInfluxObservation(obs *weatherUndergroundObservation) error {

}

func init() {
	rootCmd.AddCommand(ETLCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	ETLCmd.PersistentFlags().String("foo", "", "A help for foo")
	ETLCmd.PersistentFlags().StringArray("stations", nil, "List of PWS Station IDs")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	ETLCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
