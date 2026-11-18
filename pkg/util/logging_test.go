package util_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

var _ = Describe("MongoLogSink", func() {
	It("covers New, Info, and Error paths including nil logger", func() {
		nilSink := MongoLogSink{}.New()
		nilSink.Info(1, "ignored")
		nilSink.Error(errors.New("ignored"), "ignored")

		AddBeamFor(MongoLogSink{}.New())
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

		sink := GetBeamScopedInstance[*MongoLogSink]()
		sink.Info(1, "debug message", "k", "v")
		sink.Info(2, "trace message", "k", "v")
		sink.Error(errors.New("driver err"), "error with err", "k", "v")
		sink.Error(nil, "error without err", "k", "v")
	})
})
