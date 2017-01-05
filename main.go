package main

import (
	"github.com/Sirupsen/logrus"

	"github.com/rybit/extractor/cmd"
)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		logrus.Fatalf("Failed to execute command: %v", err)
	}
}
