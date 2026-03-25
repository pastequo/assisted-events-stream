package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const prefix = "ASSISTED_EVENTS_STREAMS_MIGRATIONS"

// Parse reads the configuration file given as parameter.
func ParseMigration(confFile string) (Migration, error) {
	ret := Migration{}

	viper.SetEnvPrefix(prefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv() // read in environment variables that match

	if len(confFile) > 0 {
		viper.SetConfigFile(confFile)

		err := viper.ReadInConfig()
		if err != nil {
			return ret, fmt.Errorf("failed to read config file %v: %w", confFile, err)
		}
	}

	err := viper.Unmarshal(&ret)
	if err != nil {
		return ret, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return ret, nil
}
