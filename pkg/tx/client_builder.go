package tx

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays/pkg/lang"
)

const (
	defaultHost           = "127.0.0.1"
	defaultPort           = 27017
	defaultConnectTimeout = 10 * time.Second
	defaultSelectTimeout  = 10 * time.Second
)

// MongoClientBuilder provides a fluent builder API for creating configured [*mongo.Client]
// and [*mongo.Database] instances inside Rays [Configuration] structs without relying on global
// ConfigurationProperties.
type MongoClientBuilder struct {
	logSink                options.LogSink
	uri                    string
	host                   string
	username               string
	password               string
	authSource             string
	customizers            []func(*options.ClientOptions)
	port                   int
	connectTimeout         time.Duration
	serverSelectionTimeout time.Duration
	maxPoolSize            uint64
	minPoolSize            uint64
	directConnection       bool
}

// NewMongoClientBuilder creates a new [*MongoClientBuilder] with sensible defaults
// (host 127.0.0.1, port 27017, 10s connect & server selection timeouts).
func NewMongoClientBuilder() *MongoClientBuilder {
	return &MongoClientBuilder{
		host:                   defaultHost,
		port:                   defaultPort,
		connectTimeout:         defaultConnectTimeout,
		serverSelectionTimeout: defaultSelectTimeout,
	}
}

// WithURI sets the full MongoDB connection URI (overrides host and port when non-empty).
func (this *MongoClientBuilder) WithURI(uri string) *MongoClientBuilder {
	this.uri = uri
	return this
}

// WithHost sets the MongoDB hostname used when no URI is provided.
func (this *MongoClientBuilder) WithHost(host string) *MongoClientBuilder {
	this.host = host
	return this
}

// WithPort sets the MongoDB port used when no URI is provided.
func (this *MongoClientBuilder) WithPort(port int) *MongoClientBuilder {
	this.port = port
	return this
}

// WithCredentials configures authentication credentials.
func (this *MongoClientBuilder) WithCredentials(username, password, authSource string) *MongoClientBuilder {
	this.username = username
	this.password = password
	this.authSource = authSource
	return this
}

// WithConnectTimeout sets the connection and startup ping timeout.
func (this *MongoClientBuilder) WithConnectTimeout(d time.Duration) *MongoClientBuilder {
	this.connectTimeout = d
	return this
}

// WithServerSelectionTimeout sets the server selection timeout.
func (this *MongoClientBuilder) WithServerSelectionTimeout(d time.Duration) *MongoClientBuilder {
	this.serverSelectionTimeout = d
	return this
}

// WithMaxPoolSize sets the maximum connection pool size.
func (this *MongoClientBuilder) WithMaxPoolSize(size uint64) *MongoClientBuilder {
	this.maxPoolSize = size
	return this
}

// WithMinPoolSize sets the minimum connection pool size.
func (this *MongoClientBuilder) WithMinPoolSize(size uint64) *MongoClientBuilder {
	this.minPoolSize = size
	return this
}

// WithDirectConnection enables or disables direct single-host connection mode.
func (this *MongoClientBuilder) WithDirectConnection(direct bool) *MongoClientBuilder {
	this.directConnection = direct
	return this
}

// WithLogSink configures a custom MongoDB driver [options.LogSink] (such as the auto-configured [util.MongoLogSink]).
func (this *MongoClientBuilder) WithLogSink(sink options.LogSink) *MongoClientBuilder {
	this.logSink = sink
	return this
}

// WithOptions appends a customizer callback to mutate [*options.ClientOptions] prior to connecting.
func (this *MongoClientBuilder) WithOptions(fn func(*options.ClientOptions)) *MongoClientBuilder {
	if fn != nil {
		this.customizers = append(this.customizers, fn)
	}
	return this
}

func (this *MongoClientBuilder) resolvedURI() string {
	if this.uri != "" {
		return this.uri
	}
	host := this.host
	if host == "" {
		host = defaultHost
	}
	port := this.port
	if port <= 0 {
		port = defaultPort
	}
	u := url.URL{
		Scheme: "mongodb",
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/",
	}
	return u.String()
}

func (this *MongoClientBuilder) resolvedConnectTimeout() time.Duration {
	if this.connectTimeout > 0 {
		return this.connectTimeout
	}
	return defaultConnectTimeout
}

// BuildOptions constructs the [*options.ClientOptions] from the builder settings.
func (this *MongoClientBuilder) BuildOptions() *options.ClientOptions {
	opts := options.Client().ApplyURI(this.resolvedURI())

	if this.username != "" || this.password != "" {
		opts.SetAuth(options.Credential{
			Username:   this.username,
			Password:   this.password,
			AuthSource: this.authSource,
		})
	}

	opts.SetConnectTimeout(this.resolvedConnectTimeout())

	if this.serverSelectionTimeout > 0 {
		opts.SetServerSelectionTimeout(this.serverSelectionTimeout)
	} else {
		opts.SetServerSelectionTimeout(defaultSelectTimeout)
	}

	if this.maxPoolSize > 0 {
		opts.SetMaxPoolSize(this.maxPoolSize)
	}
	if this.minPoolSize > 0 {
		opts.SetMinPoolSize(this.minPoolSize)
	}
	if this.directConnection {
		opts.SetDirect(true)
	}

	if this.logSink != nil {
		loggerOpts := options.Logger().
			SetSink(this.logSink).
			SetComponentLevel(options.LogComponentCommand, options.LogLevelDebug)
		opts.SetLoggerOptions(loggerOpts)
	}

	for _, customizer := range this.customizers {
		customizer(opts)
	}

	return opts
}

// Build connects to MongoDB, verifies connectivity with Ping, and returns the [*mongo.Client].
func (this *MongoClientBuilder) Build(ctx ...context.Context) (*mongo.Client, Error) {
	baseCtx := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		baseCtx = ctx[0]
	}
	connectCtx, cancel := context.WithTimeout(baseCtx, this.resolvedConnectTimeout())
	defer cancel()

	client, err := mongo.Connect(connectCtx, this.BuildOptions())
	if err != nil {
		return nil, NewWrappedError("MongoConnectionError", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, NewWrappedError("MongoConnectionError", err)
	}

	return client, nil
}

// BuildDatabase connects to MongoDB and returns the [*mongo.Database] for the given database name.
func (this *MongoClientBuilder) BuildDatabase(database string, ctx ...context.Context) (*mongo.Database, Error) {
	client, err := this.Build(ctx...)
	if err != nil {
		return nil, err
	}
	return client.Database(database), nil
}
