package logger

import (
	"go.uber.org/zap/zapcore"
)

// Config represents the configuration for creating a zap.Logger.
type Config struct {
	Format       string
	Level        zapcore.Level
	SuppressLogo bool
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		Format: "auto",
		Level:  zapcore.InfoLevel,
	}
}
