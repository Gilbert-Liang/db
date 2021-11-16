package config

import (
	"time"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "CnosDB"

	// DefaultMaxBodySize is the default maximum size of a client request body, in bytes. Specify 0 for no limit.
	DefaultMaxBodySize = 25e6

	// DefaultEnqueuedWriteTimeout is the maximum time a write request can wait to be processed.
	DefaultEnqueuedWriteTimeout = 30 * time.Second
)

type HTTP struct {
	Enabled              bool
	BindAddress          string
	LogEnabled           bool
	WriteTracing         bool
	Realm                string
	MaxBodySize          int
	EnqueuedWriteTimeout time.Duration
}

func NewHTTPConfig() HTTP {
	return HTTP{
		Enabled:              true,
		BindAddress:          DefaultBindAddress,
		LogEnabled:           true,
		Realm:                DefaultRealm,
		MaxBodySize:          DefaultMaxBodySize,
		EnqueuedWriteTimeout: DefaultEnqueuedWriteTimeout,
	}
}
