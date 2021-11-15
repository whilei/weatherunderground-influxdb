package cmd

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestForecastFields(t *testing.T) {
	dataFile := filepath.Join("..", "forecast-example.json")

	b, err := ioutil.ReadFile(dataFile)
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range forecastFields {
		res := gjson.GetBytes(b, f)

		resSl := res.Value().([]interface{})
		t.Logf("%s => %v", f, resSl)
	}

	t.Logf("Expires: %v", time.Unix(gjson.GetBytes(b, "expirationTimeUtc.0").Int(), 0))
}

func TestTimeAfterZero(t *testing.T) {
	var initTime time.Time
	t.Logf("%v", time.Now().After(initTime))
}
