package tx

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"strings"
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
	connectTimeout         time.Duration
	serverSelectionTimeout time.Duration
	maxPoolSize            uint64
	minPoolSize            uint64
	port                   int
	directConnection       bool
	hasDirectConnection    bool
	hasMaxPoolSize         bool
	hasMinPoolSize         bool
	commandLogging         bool
}

// NewMongoClientBuilder creates a new [*MongoClientBuilder] with sensible defaults
// (host 127.0.0.1, port 27017). Timeouts default to 10s unless specified via URI or setters.
func NewMongoClientBuilder() *MongoClientBuilder {
	return &MongoClientBuilder{
		host: defaultHost,
		port: defaultPort,
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
	this.hasMaxPoolSize = true
	return this
}

// WithMinPoolSize sets the minimum connection pool size.
func (this *MongoClientBuilder) WithMinPoolSize(size uint64) *MongoClientBuilder {
	this.minPoolSize = size
	this.hasMinPoolSize = true
	return this
}

// WithDirectConnection enables or disables direct single-host connection mode.
func (this *MongoClientBuilder) WithDirectConnection(direct bool) *MongoClientBuilder {
	this.directConnection = direct
	this.hasDirectConnection = true
	return this
}

// WithLogSink configures a custom MongoDB driver [options.LogSink] (such as the auto-configured [util.MongoLogSink]).
func (this *MongoClientBuilder) WithLogSink(sink options.LogSink) *MongoClientBuilder {
	this.logSink = sink
	return this
}

// WithCommandLogging explicitly enables or disables MongoDB command payload logging (debug level).
// By default, command payload logging is disabled to protect sensitive query data.
func (this *MongoClientBuilder) WithCommandLogging(enabled bool) *MongoClientBuilder {
	this.commandLogging = enabled
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
	cleanHost := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	u := url.URL{
		Scheme: "mongodb",
		Host:   net.JoinHostPort(cleanHost, strconv.Itoa(port)),
		Path:   "/",
	}
	return u.String()
}

func (this *MongoClientBuilder) resolvedConnectTimeout(opts *options.ClientOptions) time.Duration {
	if this.connectTimeout > 0 {
		return this.connectTimeout
	}
	if opts != nil && opts.ConnectTimeout != nil {
		return *opts.ConnectTimeout
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

	if this.connectTimeout > 0 {
		opts.SetConnectTimeout(this.connectTimeout)
	} else if opts.ConnectTimeout == nil {
		opts.SetConnectTimeout(defaultConnectTimeout)
	}

	if this.serverSelectionTimeout > 0 {
		opts.SetServerSelectionTimeout(this.serverSelectionTimeout)
	} else if opts.ServerSelectionTimeout == nil {
		opts.SetServerSelectionTimeout(defaultSelectTimeout)
	}

	if this.hasMaxPoolSize {
		opts.SetMaxPoolSize(this.maxPoolSize)
	}
	if this.hasMinPoolSize {
		opts.SetMinPoolSize(this.minPoolSize)
	}
	if this.hasDirectConnection {
		opts.SetDirect(this.directConnection)
	}

	if this.logSink != nil {
		loggerOpts := options.Logger().SetSink(this.logSink)
		if this.commandLogging {
			loggerOpts.SetComponentLevel(options.LogComponentCommand, options.LogLevelDebug)
		}
		opts.SetLoggerOptions(loggerOpts)
	}

	for _, customizer := range this.customizers {
		customizer(opts)
	}

	return opts
}

// Build connects to MongoDB, verifies connectivity with Ping, and returns the [*mongo.Client].
func (this *MongoClientBuilder) Build(ctx ...context.Context) (*mongo.Client, Error) {
	opts := this.BuildOptions()
	timeout := this.resolvedConnectTimeout(opts)

	baseCtx := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		baseCtx = ctx[0]
	}
	connectCtx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	client, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return nil, NewWrappedError("MongoConnectionError", err)
	}

	if err := client.Ping(connectCtx, nil); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(baseCtx, timeout)
		defer cleanupCancel()
		_ = client.Disconnect(cleanupCtx)
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
