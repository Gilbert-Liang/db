package meta

import (
	"errors"
)

// Config represents the meta configuration.
type Config struct {
	Dir                  string
	TimeToLiveAutoCreate bool
}

// NewConfig builds a new configuration with default values.
func NewConfig() *Config {
	return &Config{
		TimeToLiveAutoCreate: true,
	}
}

func (c *Config) Validate() error {
	if c.Dir == "" {
		return errors.New("Meta.Dir must be specified")
	}
	return nil
}
