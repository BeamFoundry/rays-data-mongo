package tx_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTx(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg/Tx Mongo Suite")
}
