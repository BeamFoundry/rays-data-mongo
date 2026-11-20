package util

import (
	"context"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
)

// DefaultPageSize is the default number of items per page when pageSize <= 0 in Paginate.
const DefaultPageSize = 10

// Paginate returns a [MongoQueryFunc] that calculates skip and limit from a 1-indexed page number and page size.
// If page <= 0, page defaults to 1.
// If pageSize <= 0, pageSize defaults to [DefaultPageSize].
// If (page - 1) * pageSize overflows int64, a PaginationOverflowError is attached to the query.
func Paginate(page, pageSize int) MongoQueryFunc {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	return func(q *MongoQuery) *MongoQuery {
		p := int64(page - 1)
		ps := int64(pageSize)
		if p > math.MaxInt64/ps {
			err := NewError("PaginationOverflowError", "pagination offset overflow", "page", page, "pageSize", pageSize)
			return q.AddError(err)
		}
		offset := p * ps
		return q.Skip(offset).Limit(ps)
	}
}

// WithLimit returns a [MongoQueryFunc] that limits the maximum number of documents returned.
func WithLimit(limit int64) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Limit(limit)
	}
}

// Limit is an alias for [WithLimit].
func Limit(limit int64) MongoQueryFunc {
	return WithLimit(limit)
}

// WithSkip returns a [MongoQueryFunc] that skips the specified number of documents.
func WithSkip(skip int64) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Skip(skip)
	}
}

// WithOffset is an alias for [WithSkip].
func WithOffset(offset int64) MongoQueryFunc {
	return WithSkip(offset)
}

// OrderByAsc returns a [MongoQueryFunc] that sorts results by the given BSON field in ascending order.
func OrderByAsc(field string) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.OrderByAsc(field)
	}
}

// OrderByDesc returns a [MongoQueryFunc] that sorts results by the given BSON field in descending order.
func OrderByDesc(field string) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.OrderByDesc(field)
	}
}

// WithSort returns a [MongoQueryFunc] that applies an arbitrary MongoDB sort document.
func WithSort(sort any) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Order(sort)
	}
}

// WithOrder is an alias for [WithSort].
func WithOrder(sort any) MongoQueryFunc {
	return WithSort(sort)
}

// WithSelect returns a [MongoQueryFunc] that includes only the specified BSON fields.
func WithSelect(fields ...string) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Select(fields...)
	}
}

// WithOmit returns a [MongoQueryFunc] that excludes the specified BSON fields.
func WithOmit(fields ...string) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Omit(fields...)
	}
}

// WithProjection returns a [MongoQueryFunc] that applies a custom BSON projection document.
func WithProjection(proj any) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Projection(proj)
	}
}

// WithWhere returns a [MongoQueryFunc] that appends a MongoDB filter document.
func WithWhere(filter any) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Where(filter)
	}
}

// WithFilter is an alias for [WithWhere].
func WithFilter(filter any) MongoQueryFunc {
	return WithWhere(filter)
}

// WithOr returns a [MongoQueryFunc] that appends an $or filter condition.
func WithOr(filters ...any) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Or(filters...)
	}
}

// WithCollection returns a [MongoQueryFunc] that overrides the target collection name.
func WithCollection(name string) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Collection(name)
	}
}

// WithHint returns a [MongoQueryFunc] that specifies an index hint.
func WithHint(hint any) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Hint(hint)
	}
}

// WithMaxTime returns a [MongoQueryFunc] that sets the maximum server execution duration.
func WithMaxTime(d time.Duration) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.MaxTime(d)
	}
}

// WithCollation returns a [MongoQueryFunc] that configures string comparison collation.
func WithCollation(collation *options.Collation) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Collation(collation)
	}
}

// WithQueryContext returns a [MongoQueryFunc] that overrides the query execution [context.Context].
func WithQueryContext(ctx context.Context) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.WithContext(ctx)
	}
}

// Combine merges multiple [MongoQueryFunc] helpers into a single sequential scope.
func Combine(funcs ...MongoQueryFunc) MongoQueryFunc {
	return func(q *MongoQuery) *MongoQuery {
		return q.Scopes(funcs...)
	}
}
