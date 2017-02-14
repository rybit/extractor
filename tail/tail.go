package tail

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_metrics"

	"strconv"

	"fmt"

	"github.com/rybit/extractor/conf"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/extractor/stats"
)

const nanoInMsec int64 = 1000000

var zero time.Time

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
	shutdown := make(chan bool)
	go func() {
		for {
			select {
			case line := <-lines:
				stats.Increment("lines_seen")
				text := strings.TrimSpace(line)
				if len(text) > 0 {
					for _, m := range defs {
						l := log.WithField("metric_name", m.Name)
						if fields, rawDims, ok := parsing.ParseLine(text, m.Fields, log); ok {
							name, err := extractName(m.Name, m.MungeDef, fields)
							if err != nil {
								l.WithError(err).Warn("Failed to extract a name")
								stats.Increment("failed_extraction")
								continue
							}

							value, err := extractValue(m.ValueField, fields)
							if err != nil {
								l.WithError(err).Warnf("Failed to extract value from position %d.", *m.ValueField)
								stats.Increment("failed_extraction")
								continue
							}

							when, err := extractTimestamp(m.TimestampField, m.TimestampFormat, fields)
							if err != nil {
								l.WithError(err).Warn("Failed to extract a timestamp")
								stats.Increment("failed_extraction")
								continue
							}
							dims := convert(rawDims)
							for _, field := range fields {
								(*dims)[field.Label] = field.Value
							}

							c := metrics.NewCounter(name, nil)
							if !when.IsZero() {
								c.SetTimestamp(when)
							}
							c.CountN(value, dims)

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
				return
			}
		}
	}()

	return shutdown
}

func convert(in map[string]interface{}) *metrics.DimMap {
	out := metrics.DimMap{}
	for k, v := range in {
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

func extractValue(idx *int, fields map[int]parsing.ParsedField) (int64, error) {
	if idx == nil {
		return 1, nil
	}

	raw, ok := fields[*idx]
	if !ok {
		return 0, fmt.Errorf("Index %d is out of range of the parsed fields", *idx)
	}

	delete(fields, *idx)

	switch val := raw.Value.(type) {
	case string:
		asInt, err := strconv.Atoi(val)
		if err != nil {
			return 0, fmt.Errorf("Failed to convert %s from a string to an int: %v", val, err)
		}
		return int64(asInt), nil
	case int:
		return int64(raw.Value.(int)), nil
	case int32:
		return int64(raw.Value.(int32)), nil
	case int64:
		return val, nil

	}

	return 0, fmt.Errorf("Failed to convert %v to either a string or int", raw)
}

func extractName(root string, def *conf.MungeDef, fields map[int]parsing.ParsedField) (string, error) {
	if def == nil {
		return root, nil
	}

	raw, ok := fields[def.FieldNumber]
	if !ok {
		return "", fmt.Errorf("Index %d is out of range of the parsed fields", def.FieldNumber)
	}

	if !def.KeepDimension {
		delete(fields, def.FieldNumber)
	}

	joiner := def.Joiner
	if joiner == "" {
		joiner = "_"
	}

	return fmt.Sprintf("%s%s%s", root, joiner, raw.Value.(string)), nil
}

func extractTimestamp(idx *int, format string, fields map[int]parsing.ParsedField) (time.Time, error) {
	if idx == nil {
		return zero, nil
	}

	raw, ok := fields[*idx]
	if !ok {
		return zero, fmt.Errorf("Index %d is out of range of the parsed fields", *idx)
	}

	delete(fields, *idx)

	if format == "" {
		format = "msec"
	}

	switch format {
	case "msec", "nano", "sec":
		// could be a number - convert it to int64
		if num, err := strconv.ParseInt(raw.Value.(string), 10, 64); err == nil {
			switch format {
			case "nano":
				return time.Unix(0, num), nil
			case "msec":
				return time.Unix(0, num*nanoInMsec), nil
			case "sec":
				return time.Unix(num, 0), nil
			}
		}
	default:
		return time.Parse(format, raw.Value.(string))
	}

	return zero, fmt.Errorf("Failed to parse timestamp from '%v'", raw.Value)
}
