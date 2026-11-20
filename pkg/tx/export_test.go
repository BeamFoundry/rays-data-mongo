package tx

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// ExportIsUnknownTransactionCommitResult exposes isUnknownTransactionCommitResult for testing.
func ExportIsUnknownTransactionCommitResult(err error) bool {
	return isUnknownTransactionCommitResult(err)
}

// ExportIsServerSelectionError exposes isServerSelectionError for testing.
func ExportIsServerSelectionError(err error) bool {
	return isServerSelectionError(err)
}

// ExportResolvedConnectTimeout exposes resolvedConnectTimeout for testing.
func (this *MongoClientBuilder) ExportResolvedConnectTimeout(opts *options.ClientOptions) time.Duration {
	return this.resolvedConnectTimeout(opts)
}
