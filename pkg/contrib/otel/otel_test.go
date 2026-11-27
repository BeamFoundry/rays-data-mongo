package otel_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	_ "github.com/BeamFoundry/rays"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/testing"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/contrib/otel"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestOtel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Otel Suite")
}

var recordedTP oteltrace.TracerProvider

type testTPConfig struct {
	Configuration
}

func (this *testTPConfig) TracerProvider() oteltrace.TracerProvider {
	return recordedTP
}

type testCustomizer struct {
	called bool
}

var _ MongoOtelCustomizer = &testCustomizer{}

func (this *testCustomizer) CustomizeMongoOtel(opts []otelmongo.Option) []otelmongo.Option {
	this.called = true
	return opts
}

var _ = Describe("MongoOtelConfiguration", func() {
	emitCommand := func(monitor *event.CommandMonitor, requestID int64) {
		ctx := context.Background()
		monitor.Started(ctx, &event.CommandStartedEvent{
			CommandName:  "find",
			DatabaseName: "testdb",
			RequestID:    requestID,
			Command:      bson.Raw{},
		})
		monitor.Succeeded(ctx, &event.CommandSucceededEvent{
			CommandFinishedEvent: event.CommandFinishedEvent{
				CommandName: "find",
				RequestID:   requestID,
				Duration:    time.Millisecond,
			},
		})
	}

	It("provides a CommandMonitor configured with TracerProvider, options, customizers, and ClientOptionsCustomizer", func() {
		Expect(GetContainer().LoadConfigAt(map[string]any{
			"commandAttributeDisabled": true,
		}, "rays.data.mongo.otel")).NotTo(HaveOccurred())

		sr := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
		recordedTP = tp
		DeferCleanup(func() {
			recordedTP = nil
			Expect(tp.Shutdown(context.Background())).To(Succeed())
		})

		customizer := &testCustomizer{}
		AddBeamFor(&testTPConfig{})
		AddBeamFor(customizer)
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).To(Succeed())

		Expect(customizer.called).To(BeTrue())

		monitor := GetBeamScopedInstance[*event.CommandMonitor]()
		Expect(monitor).NotTo(BeNil())

		clientOpts := options.Client()
		ClientOptionsCustomizer(monitor)(clientOpts)
		Expect(clientOpts.Monitor).To(Equal(monitor))

		emitCommand(monitor, 1)
		Expect(tp.ForceFlush(context.Background())).To(Succeed())

		spans := sr.Ended()
		Expect(spans).To(HaveLen(1))
		for _, attr := range spans[0].Attributes() {
			Expect(string(attr.Key)).NotTo(Equal("db.statement"))
		}
	})

	It("surfaces non-KeyNotFoundError from rays.data.mongo.otel config", func() {
		Expect(GetContainer().LoadConfigAt(map[string]any{
			"commandAttributeDisabled": "not-a-bool",
		}, "rays.data.mongo.otel")).NotTo(HaveOccurred())

		GetContainer().EnableScanning()
		err := GetContainer().Initialize()
		Expect(err).To(HaveOccurred())
	})
})
