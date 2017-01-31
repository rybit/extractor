package tail

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_metrics"

	"github.com/rybit/extractor/conf"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/extractor/stats"
)

func FollowForever(config *conf.Config, path string, log *logrus.Entry) (chan bool, chan bool) {
	stop := make(chan bool)
	stopped := make(chan bool)

	go func() {
		where := io.SeekEnd
		for {
			select {
			case <-stop:
				log.Info("Shutting down by request")
				stopped <- true
				return
			default:
				log.Info("Starting to process file")
				ProcessFile(config, path, log, where, true)
				where = io.SeekStart

				log.Info("Finished processing the file, it was probably rolled. Going to retry")
			}
		}
	}()

	return stop, stopped
}

func ProcessFile(config *conf.Config, path string, log *logrus.Entry, seek int, follow bool) chan bool {
	if path == "" {
		log.Fatal("Must provide a path to consume")
	}

	notFound := true
	for notFound {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Warnf("File %s does not exist, will check again in %d seconds", path, config.RetrySec)
			<-time.After(time.Duration(config.RetrySec) * time.Second)
		} else {
			notFound = false
		}
	}

	log.WithField("path", path).Info("Found file to process")

	fromEnd := true
	if seek == io.SeekStart {
		fromEnd = false
	}

	consumer := newConsumer(path, log.WithField("component", "watcher"))
	consumer.FromEnd = fromEnd
	consumer.Follow = follow
	go consumer.consume()

	log.WithFields(logrus.Fields{
		"position": asString(seek),
		"subject":  config.Subject,
	}).Info("Starting to tail file")
	return processLines(consumer.Out, config.Metrics, log)
}

func processLines(lines chan string, defs []conf.MetricDef, log *logrus.Entry) chan bool {
	counters := make(map[string]metrics.Counter)
	for _, m := range defs {
		counters[m.Name] = metrics.NewCounter(m.Name, nil)
	}

	zero := time.Time{}
	shutdown := make(chan bool)
	go func() {
		for {
			select {
			case line := <-lines:
				stats.Increment("lines_seen")
				text := strings.TrimSpace(line)
				if len(text) > 0 {
					for _, m := range defs {
						c := counters[m.Name]
						parsed, ok := parsing.ParseLine(text, m.Fields, log)
						if ok {
							if parsed.Timestamp != nil {
								c.SetTimestamp(*parsed.Timestamp)
							}

							c.CountN(parsed.Value, convert(&parsed.Dims))
							c.SetTimestamp(zero)
							stats.Increment("metrics_published")
						} else {
							stats.Increment("failed_extraction")
						}
					}
				} else {
					stats.Increment("blank_lines_seen")
				}
			case <-shutdown:
				log.Info("Got shutdown message")
			}
		}
	}()

	return shutdown
}

func convert(in *map[string]interface{}) *metrics.DimMap {
	out := metrics.DimMap{}
	for k, v := range *in {
		out[k] = v
	}
	return &out
}

func asString(seek int) string {
	switch seek {
	case io.SeekEnd:
		return "end"
	case io.SeekStart:
		return "start"
	case io.SeekCurrent:
		return "current"
	}

	return "unknown"
}
