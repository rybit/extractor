package stats

import (
	"sync"
	"time"

	"github.com/rybit/nats_metrics"
)

type Config struct {
	Interval int    `mapstructure:"report_sec"`
	Subject  string `mapstructure:"subject"`
	Prefix   string `mpastructure:"prefix"`
}

var statLock sync.Mutex
var stats map[string]int64

func ReportStats(config *Config, dims *map[string]interface{}) {
	if config == nil || config.Interval == 0 {
		return
	}

	dimMap := metrics.DimMap{}
	for k, v := range dimMap {
		dimMap[k] = v
	}

	go func() {
		ticks := time.Tick(time.Duration(config.Interval) * time.Second)
		for range ticks {
			go func() {
				statLock.Lock()
				for k, v := range stats {
					name := config.Prefix
					if name != "" {
						name += "."
					}
					name += k

					metrics.NewGauge(name, nil).Set(v, &dimMap)
				}
				statLock.Unlock()
			}()
		}
	}()
}

func Increment(key string) {
	go func() {
		statLock.Lock()
		defer statLock.Unlock()
		val, ok := stats[key]
		if !ok {
			val = 0
		}
		stats[key] = val + 1
	}()
}
