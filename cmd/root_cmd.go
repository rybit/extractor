package cmd

import (
	"github.com/Sirupsen/logrus"
	"github.com/rybit/nats_logrus_hook"
	"github.com/rybit/nats_metrics"
	"github.com/spf13/cobra"

	"encoding/json"

	"io"

	"github.com/rybit/extractor/conf"
	"github.com/rybit/extractor/messaging"
	"github.com/rybit/extractor/stats"
	"github.com/rybit/extractor/tail"
)

const defaultRetrySec = 5
const defaultReportSec = 60

var defaultDelim string
var cmdLineFields = []string{}

func RootCmd() *cobra.Command {
	rootCmd := &cobra.Command{}

	followCmd := &cobra.Command{
		Short: "follow <path>",
		Use:   "follow",
		Run:   run,
	}

	rootCmd.AddCommand(followCmd, processCmd, versionCmd)

	rootCmd.PersistentFlags().StringP("config", "c", "config.json", "a configruation file to use")
	rootCmd.PersistentFlags().StringVarP(&defaultDelim, "delim", "d", "=", "the delimiter to use for fields")
	rootCmd.PersistentFlags().StringSliceVarP(&cmdLineFields, "field", "f", cmdLineFields, "field overrides in the form: 'TODO'")

	return rootCmd
}

func run(cmd *cobra.Command, args []string) {
	config, log := setup(cmd)

	if len(args) != 1 {
		log.Fatal("Must provide a path to consume")
	}

	if config.Dims != nil {
		for k, v := range *config.Dims {
			metrics.AddDimension(k, v)
		}
	}
	stats.ReportStats(config.ReportConf, log, config.Dims)
	tail.ProcessFile(config, args[0], log, io.SeekEnd, true)

	select {}
}

func setup(cmd *cobra.Command) (*conf.Config, *logrus.Entry) {
	config, err := conf.LoadConfig(cmd)
	if err != nil {
		logrus.Fatalf("Failed to load configuration: %v", err)
	}
	log, err := conf.ConfigureLogging(&config.LogConf)
	if err != nil {
		log.Fatalf("Failed to configure logging : %v", err)
	}

	if len(config.Metrics) == 0 {
		log.Fatal("Must provide at least one metric to extract")
	}

	if config.ReportConf != nil {
		if config.ReportConf.Interval == 0 {
			config.ReportConf.Interval = defaultReportSec
		}
	}

	if config.NatsConf != nil {
		nc, err := messaging.ConnectToNats(config.NatsConf, messaging.ErrorHandler(log))
		if err != nil {
			log.WithError(err).Fatal("Failed to connect to nats")
		}

		log.Debug("Connected to NATS")
		if err := metrics.Init(nc, config.Subject); err != nil {
			log.WithError(err).Fatal("Failed to configure metrics lib")
		}
		log.WithField("metrics_subject", config.Subject).Debug("Configured metrics lib")

		if config.NatsConf.LogSubject != "" {
			logrus.AddHook(nhook.NewNatsHook(nc, config.NatsConf.LogSubject))
			log.WithField("log_subject", config.NatsConf.LogSubject).Debug("Configured nats hook into logrus")
		}
	} else {
		log.Debug("No nats config specified - going to output using logger")
		metrics.Init(nil, "nowhere")
		metrics.Trace(func(m *metrics.RawMetric) {
			bs, err := json.Marshal(m)
			if err == nil {
				log.Info(string(bs))
			}
		})
	}

	if config.RetrySec == 0 {
		config.RetrySec = defaultRetrySec
	}

	return config, log
}
