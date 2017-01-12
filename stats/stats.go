package stats

import (
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_metrics"
)

type Config struct {
	Interval int    `mapstructure:"report_sec"`
	Subject  string `mapstructure:"subject"`
	Prefix   string `mpastructure:"prefix"`
}

var statLock sync.Mutex
var stats = make(map[string]int64)

func ReportStats(config *Config, log *logrus.Entry, dims *map[string]interface{}) {
	if config == nil || config.Interval == 0 {
		log.Debug("Skipping stats reporting because it is configured off")
		return
	}

	dimMap := metrics.DimMap{}
	for k, v := range *dims {
		dimMap[k] = v
	}

	go func() {
		log.WithFields(logrus.Fields{
			"interval":      config.Interval,
			"subject":       config.Subject,
			"metric_prefix": config.Prefix,
		}).Infof("Starting to report stats every %d seconds", config.Interval)
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
