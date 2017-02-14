package tail

import (
	"testing"

	"time"

	"fmt"

	"github.com/rybit/extractor/conf"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/extractor/stats"
	"github.com/rybit/nats_metrics"
	"github.com/stretchr/testify/assert"
)

var two = 2

func TestReadLines(t *testing.T) {
	stats.Reset()
	lines := make(chan string)
	defs := []conf.MetricDef{
		{
			Name: "testing-1",
			MungeDef: &conf.MungeDef{
				FieldNumber: 3,
				Joiner:      "-",
			},
			Fields: []parsing.FieldDef{

				{Position: 0, Type: parsing.BoolType},
				{Position: 1},
				{Position: 2},
				{Position: 3, Type: parsing.URLType},
			},
			ValueField: &two,
		},
	}

	shutdown := processLines(lines, defs, tl)

	sent := make(chan *metrics.RawMetric)
	metrics.Init(nil, "nowhere")
	metrics.Trace(func(rm *metrics.RawMetric) {
		sent <- rm
	})

	lines <- "some-bool=true some-string=batman-rules some-number=123 some-domain=https://gotham.com/villians"
	lines <- "     " // only whitespace
	lines <- "some-bool=false some-string=joker-sucks some-number=notanumber some-domain=https://gotham.com/villians"
	lines <- "some-bool=false some-string=joker-sucks some-number=123"

	select {
	case rm := <-sent:
		assert.Equal(t, "testing-1-gotham", rm.Name)
		assert.Equal(t, metrics.CounterType, rm.Type)
		assert.EqualValues(t, 123, rm.Value)
		assert.Equal(t, 4, len(rm.Dims))
		assert.Equal(t, true, rm.Dims["some-bool"])
		assert.Equal(t, "com", rm.Dims["tld"])
		assert.Equal(t, "https", rm.Dims["scheme"])
		assert.Equal(t, "batman-rules", rm.Dims["some-string"])
		shutdown <- true
	case <-time.After(time.Second):
		assert.FailNow(t, "Failed to get a new metric inside of a second")
	}

	// validate stats
	assert.EqualValues(t, 1, stats.Get("metrics_published"))
	assert.EqualValues(t, 2, stats.Get("failed_extraction"))
	assert.EqualValues(t, 1, stats.Get("blank_lines_seen"))
	assert.EqualValues(t, 4, stats.Get("lines_seen"))
}

func TestReadLinesWithTimestamp(t *testing.T) {
	stats.Reset()
	lines := make(chan string)
	defs := []conf.MetricDef{
		{
			Name: "testing-1",
			Fields: []parsing.FieldDef{

				{Position: 0, Type: parsing.BoolType},
				{Position: 1},
				{Position: 2},
			},
			TimestampField:  &two,
			TimestampFormat: "nano",
		},
	}

	shutdown := processLines(lines, defs, tl)

	sent := make(chan *metrics.RawMetric)
	metrics.Init(nil, "nowhere")
	metrics.Trace(func(rm *metrics.RawMetric) {
		sent <- rm
	})

	when := time.Now()
	lines <- fmt.Sprintf("some-bool=true some-string=batman-rules some-time=%d", when.UnixNano())
	lines <- "some-bool=true some-string=batman-rules some-time=notanumber"

	select {
	case rm := <-sent:
		assert.Equal(t, "testing-1", rm.Name)
		assert.Equal(t, metrics.CounterType, rm.Type)
		assert.EqualValues(t, 1, rm.Value)
		assert.Equal(t, 2, len(rm.Dims))
		assert.Equal(t, true, rm.Dims["some-bool"])
		assert.Equal(t, "batman-rules", rm.Dims["some-string"])
		assert.Equal(t, when.UnixNano(), rm.Timestamp.UnixNano())
		shutdown <- true
	case <-time.After(time.Second):
		assert.FailNow(t, "Failed to get a new metric inside of a second")
	}

	// validate stats
	assert.EqualValues(t, 1, stats.Get("metrics_published"))
	assert.EqualValues(t, 1, stats.Get("failed_extraction"))
	assert.EqualValues(t, 0, stats.Get("blank_lines_seen"))
	assert.EqualValues(t, 2, stats.Get("lines_seen"))
}
