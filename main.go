package main

import (
	"log"

	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hpcloud/tail"
	"github.com/rybit/nats_logrus_hook"
	"github.com/rybit/nats_metrics"
	"github.com/spf13/cobra"

	"github.com/rybit/extractor/conf"
	"github.com/rybit/extractor/messaging"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/extractor/stats"
)

const defaultRetrySec = 5

var defaultDelim string

func main() {
	rootCmd := cobra.Command{
		Short: "streamer",
		Run:   run,
	}

	rootCmd.Flags().StringP("config", "c", "config.json", "a configruation file to use")
	rootCmd.Flags().StringVarP(&defaultDelim, "delim", "d", "=", "the delimiter to use for fields")

	if err := rootCmd.Execute(); err != nil {
		logrus.Fatalf("Failed to execute command: %v", err)
	}
}

func run(cmd *cobra.Command, args []string) {
	config, log := setup(cmd)

	fieldMap := make(map[int]parsing.FieldDef)
	for _, def := range config.Fields {
		fieldMap[def.Position] = def
	}

	for _, arg := range args {
		override := parsing.ExtractDefinition(arg, defaultDelim, log)
		if override != nil {
			log.WithFields(logrus.Fields{
				"position": override.Position,
				"label":    override.Label,
				"type":     override.Type,
				"required": override.Required,
			}).Debug("Applying field override")
			fieldMap[override.Position] = *override
		}
	}

	if len(fieldMap) == 0 {
		log.Fatal("Must provide at least one field to extract")
	}

	fields := []parsing.FieldDef{}
	for _, f := range fieldMap {
		fields = append(fields, f)
	}

	log.Debug("Extracted fields to process")

	notFound := true
	for notFound {
		if _, err := os.Stat(config.Path); os.IsNotExist(err) {
			log.Warnf("File %s does not exist, will check again in %d seconds", config.RetrySec)
			<-time.After(time.Duration(config.RetrySec) * time.Second)
		} else {
			notFound = false
		}
	}

	log.WithField("path", config.Path).Info("Found file to process")

	tailConfig := tail.Config{
		Logger:      log,
		ReOpen:      true,
		MustExist:   true,
		Location:    &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END},
		Follow:      true,
		MaxLineSize: 0, // infinite lines
	}

	log.Debug("Starting to tail file from the end")
	t, err := tail.TailFile(config.Path, tailConfig)
	if err != nil {
		log.WithField("path", config.Path).WithError(err).Fatal("Failed to create tail")
	}

	counter := metrics.NewCounter(config.MetricName, convert(config.Dims))

	stats.ReportStats(config.ReportConf, config.Dims)
	for line := range t.Lines {
		stats.Increment("lines_seen")
		text := strings.TrimSpace(line.Text)
		if len(text) > 0 {
			lineDims, ok := parsing.ParseLine(text, fields, log)
			if ok {
				counter.Count(convert(&lineDims))
				stats.Increment("lines_parsed")
			}
		} else {
			stats.Increment("blank_lines_seen")
		}
	}

	log.Info("Done with extraction ~ shutting down")
}

func convert(in *map[string]interface{}) *metrics.DimMap {
	out := metrics.DimMap{}
	for k, v := range *in {
		out[k] = v
	}
	return &out
}

func setup(cmd *cobra.Command) (*conf.Config, *logrus.Entry) {
	config, err := conf.LoadConfig(cmd)
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}

	logger, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging : %v", err)
	}
	if config.Path == "" {
		logger.Fatal("Must provide a path to process")
	}

	nc, err := messaging.ConnectToNats(&config.NatsConf, messaging.ErrorHandler(logger))
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to nats")
	}
	logger.Debug("Connected to NATS")

	if config.Subject == "" {
		logger.Fatal("Must provide a subject for metrics")
	}

	if err := metrics.Init(nc, config.Subject); err != nil {
		logger.WithError(err).Fatal("Failed to configure metrics lib")
	}
	logger.WithField("metrics_subject", config.Subject).Debug("Configured metrics lib")

	if config.NatsConf.LogSubject != "" {
		logrus.AddHook(nhook.NewNatsHook(nc, config.NatsConf.LogSubject))
		logger.WithField("log_subject", config.NatsConf.LogSubject).Debug("Configured nats hook into logrus")
	}

	if config.RetrySec == 0 {
		config.RetrySec = defaultRetrySec
	}

	return config, logger
}