package cmd

import (
	"io"

	"github.com/rybit/extractor/tail"
	"github.com/rybit/nats_metrics"
	"github.com/spf13/cobra"
)

var processCmd = &cobra.Command{
	Short: "process <path>",
	Use:   "process",
	Run:   processSingleFile,
}

func processSingleFile(cmd *cobra.Command, args []string) {
	config, log := setup(cmd)

	if len(args) != 1 {
		log.Fatal("Must provide a path to consume")
	}

	if config.Dims != nil {
		for k, v := range *config.Dims {
			metrics.AddDimension(k, v)
		}
	}

	tail.ProcessFile(config, args[0], log, io.SeekStart, false)
}
