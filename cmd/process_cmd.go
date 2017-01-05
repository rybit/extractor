package cmd

import (
	"io"

	"github.com/spf13/cobra"
)

var processCmd = &cobra.Command{
	Use: "process",
	Run: processSingleFile,
}

func processSingleFile(cmd *cobra.Command, args []string) {
	config, log := setup(cmd)

	if len(args) > 0 {
		config.Path = args[0]
		log.Debugf("Reading %s instead of config value", config.Path)
	}

	processFile(config, log, io.SeekStart, false)
}
