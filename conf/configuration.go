package conf

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/rybit/extractor/messaging"
	"github.com/rybit/extractor/parsing"
	"github.com/rybit/extractor/stats"
)

type Config struct {
	NatsConf *messaging.NatsConfig `mapstructure:"nats_conf"`
	LogConf  LoggingConfig         `mapstructure:"log_conf"`

	RetrySec int    `mapstructure:"retry_sec"`
	Subject  string `mapstructure:"subject"`

	Dims       *map[string]interface{} `mapstructure:"dims"`
	Metrics    []MetricDef             `mapstructure:"metrics"`
	ReportConf *stats.Config           `mapstructure:"stats_conf"`
}

type MetricDef struct {
	Name   string             `mapstructure:"name"`
	Fields []parsing.FieldDef `mapstructure:"fields"`
}

// LoadConfig loads the config from a file if specified, otherwise from the environment
func LoadConfig(cmd *cobra.Command) (*Config, error) {
	viper.SetConfigType("json")

	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return nil, err
	}

	viper.SetEnvPrefix("streamer")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if configFile, _ := cmd.Flags().GetString("config"); configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("./")
		viper.AddConfigPath("$HOME/.netlify/")
		viper.AddConfigPath("$HOME/.netlify/streamer")
	}

	if err := viper.ReadInConfig(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	config := new(Config)

	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	return config, nil
}
