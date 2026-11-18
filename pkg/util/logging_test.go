package util_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

type record struct {
	level string
	msg   string
	args  []any
}

type recordingLogger struct {
	Logging
	records []record
}

func (r *recordingLogger) Debug(msg string, args ...any) {
	r.records = append(r.records, record{level: "DEBUG", msg: msg, args: args})
}

func (r *recordingLogger) Trace(msg string, args ...any) {
	r.records = append(r.records, record{level: "TRACE", msg: msg, args: args})
}

func (r *recordingLogger) Error(msg string, args ...any) {
	r.records = append(r.records, record{level: "ERROR", msg: msg, args: args})
}

var _ = Describe("MongoLogSink", func() {
	It("covers New, Info, and Error paths including nil logger and recording assertions", func() {
		AddBeamFor(MongoLogSink{}.New())
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

		sink := GetBeamScopedInstance[*MongoLogSink]()
		Expect(sink).NotTo(BeNil())

		rec := &recordingLogger{}
		sink.WithLogger(rec)

		// Test nil logger returns without recording
		nilSink := MongoLogSink{}.New().WithLogger(nil)
		nilSink.Info(1, "ignored")
		nilSink.Error(errors.New("ignored"), "ignored")
		Expect(rec.records).To(BeEmpty())

		// Call sink Info level 1 (Debug)
		sink.Info(1, "debug message", "k1", "v1")
		Expect(rec.records).To(HaveLen(1))
		Expect(rec.records[0].level).To(Equal("DEBUG"))
		Expect(rec.records[0].msg).To(Equal("debug message"))
		Expect(rec.records[0].args).To(Equal([]any{"k1", "v1"}))

		// Call sink Info level 2 (Trace)
		sink.Info(2, "trace message", "k2", "v2")
		Expect(rec.records).To(HaveLen(2))
		Expect(rec.records[1].level).To(Equal("TRACE"))
		Expect(rec.records[1].msg).To(Equal("trace message"))
		Expect(rec.records[1].args).To(Equal([]any{"k2", "v2"}))

		// Call sink Error with non-nil error
		err1 := errors.New("driver err")
		sink.Error(err1, "error with err", "k3", "v3")
		Expect(rec.records).To(HaveLen(3))
		Expect(rec.records[2].level).To(Equal("ERROR"))
		Expect(rec.records[2].msg).To(Equal("error with err"))
		Expect(rec.records[2].args).To(Equal([]any{"k3", "v3", "error", err1}))

		// Call sink Error with nil error
		sink.Error(nil, "error without err", "k4", "v4")
		Expect(rec.records).To(HaveLen(4))
		Expect(rec.records[3].level).To(Equal("ERROR"))
		Expect(rec.records[3].msg).To(Equal("error without err"))
		Expect(rec.records[3].args).To(Equal([]any{"k4", "v4"}))
	})
})
