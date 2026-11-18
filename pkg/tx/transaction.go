package tx

import (
	"context"
	"time"

	"github.com/timandy/routine"
	"go.mongodb.org/mongo-driver/mongo"

	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
)

// MongoSession represents an active ThreadLocal MongoDB session and transaction context,
// or an ambient background context when no transaction is active.
// It implements [context.Context] so it can be passed directly to MongoDB driver calls.
type MongoSession struct {
	//nolint:containedctx // Wraps mongo.SessionContext for ThreadLocal transaction propagation
	ctx       context.Context
	session   mongo.Session
	err       Error
	committed bool
	aborted   bool
}

var _ context.Context = &MongoSession{}

// NewMongoSession creates a [MongoSession] bound to the given context and optional [mongo.Session].
func NewMongoSession(ctx context.Context, session mongo.Session) *MongoSession {
	//nolint:contextcheck // Fallback to background context when caller passes nil
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	return &MongoSession{
		ctx:     baseCtx,
		session: session,
	}
}

// Context returns the underlying [context.Context] (a [mongo.SessionContext] when a session is active).
func (this *MongoSession) Context() context.Context {
	if this == nil || this.ctx == nil {
		return context.Background()
	}
	return this.ctx
}

// Session returns the underlying [mongo.Session], or nil if no transaction is active.
func (this *MongoSession) Session() mongo.Session {
	if this == nil {
		return nil
	}
	return this.session
}

// Error returns any error encountered when starting or finalizing the session or transaction.
func (this *MongoSession) Error() Error {
	if this == nil {
		return nil
	}
	return this.err
}

// SetError records an initialization or lifecycle error on the session.
func (this *MongoSession) SetError(err Error) {
	if this != nil {
		this.err = err
	}
}

// Validate returns an [Error] if this session has failed or already been committed or rolled back.
func (this *MongoSession) Validate() Error {
	if this == nil {
		return nil
	}
	if this.err != nil {
		return this.err
	}
	if this.aborted {
		return NewError("TransactionError", "Transaction has already been rolled back")
	}
	if this.committed {
		return NewError("TransactionError", "Transaction has already been committed")
	}
	return nil
}

// Commit commits the active MongoDB transaction on this session.
func (this *MongoSession) Commit() Error {
	if this == nil {
		return NewError("TransactionError", "No active session")
	}
	if this.err != nil {
		return this.err
	}
	if this.session == nil {
		return NewError("TransactionError", "No active MongoDB session to commit")
	}
	if this.committed || this.aborted {
		return NewError("TransactionError", "Transaction already finalized")
	}
	if err := this.session.CommitTransaction(this.ctx); err != nil {
		this.err = NewWrappedError("TransactionError", err)
		return this.err
	}
	this.committed = true
	return nil
}

// Rollback aborts the active MongoDB transaction on this session.
func (this *MongoSession) Rollback() Error {
	if this == nil {
		return NewError("TransactionError", "No active session")
	}
	if this.session == nil {
		if this.err != nil {
			return this.err
		}
		return NewError("TransactionError", "No active MongoDB session to rollback")
	}
	if this.committed || this.aborted {
		return NewError("TransactionError", "Transaction already finalized")
	}
	if err := this.session.AbortTransaction(this.ctx); err != nil {
		this.err = NewWrappedError("TransactionError", err)
		return this.err
	}
	this.aborted = true
	return nil
}

// Abort is an alias for [MongoSession.Rollback].
func (this *MongoSession) Abort() Error {
	return this.Rollback()
}

// End closes the underlying [mongo.Session] if one is open.
func (this *MongoSession) End() {
	if this != nil && this.session != nil {
		this.session.EndSession(this.Context())
	}
}

// Deadline implements [context.Context].
func (this *MongoSession) Deadline() (time.Time, bool) {
	return this.Context().Deadline()
}

// Done implements [context.Context].
func (this *MongoSession) Done() <-chan struct{} {
	return this.Context().Done()
}

// Err implements [context.Context].
func (this *MongoSession) Err() error {
	//nolint:wrapcheck // Implements context.Context.Err contract
	return this.Context().Err()
}

// Value implements [context.Context].
func (this *MongoSession) Value(key any) any {
	return this.Context().Value(key)
}

// MongoTransactional represents the interface of a [Transactional] manager for [MongoSession].
type MongoTransactional interface {
	Transactional[MongoSession]
}

// MongoTransactionalManager manages ThreadLocal MongoDB sessions and transactions.
// It has no Rays Stereotype so multiple instances can be instantiated or registered via Configuration.
type MongoTransactionalManager struct {
	client      *mongo.Client
	logger      Logging
	threadLocal routine.ThreadLocal[*MongoSession]
}

var _ MongoTransactional = &MongoTransactionalManager{}

// NewMongoTransactionalManager creates a new [MongoTransactionalManager] for the given [*mongo.Client] and [Logging].
func NewMongoTransactionalManager(client *mongo.Client, logger Logging) *MongoTransactionalManager {
	return &MongoTransactionalManager{
		client: client,
		logger: logger,
		threadLocal: routine.NewThreadLocalWithInitial(func() *MongoSession {
			return NewMongoSession(context.Background(), nil)
		}),
	}
}

// Start begins a new MongoDB session and transaction and binds it to the current goroutine.
func (this *MongoTransactionalManager) Start(ctx context.Context) *MongoSession {
	//nolint:contextcheck // Fallback to background context when caller passes nil
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if this.logger != nil {
		this.logger.Trace("Start Transaction", "manager", "MongoTransactionalManager")
	}

	var sess mongo.Session
	if err := Try(func() {
		sess = RaiseVal(this.client.StartSession())
		RaiseErr(sess.StartTransaction())
	}); err != nil {
		ms := NewMongoSession(baseCtx, nil)
		ms.SetError(NewWrappedError("TransactionError", err))
		return ms
	}

	sessionCtx := mongo.NewSessionContext(baseCtx, sess)
	ms := NewMongoSession(sessionCtx, sess)
	this.threadLocal.Set(ms)
	return ms
}

// Get returns the active [MongoSession] for the current goroutine, or an ambient background session.
func (this *MongoTransactionalManager) Get() *MongoSession {
	if ms := this.threadLocal.Get(); ms != nil {
		return ms
	}
	return NewMongoSession(context.Background(), nil)
}

// End closes any active session on the current goroutine and clears ThreadLocal storage.
func (this *MongoTransactionalManager) End() {
	if this.logger != nil {
		this.logger.Trace("Ending Transaction", "manager", "MongoTransactionalManager")
	}
	if ms := this.threadLocal.Get(); ms != nil {
		ms.End()
	}
	this.threadLocal.Set(nil)
}

// Transactional executes action inside a ThreadLocal MongoDB transaction,
// committing on nil Error or rolling back on any returned Error or panic.
func (this *MongoTransactionalManager) Transactional(ctx context.Context, action func(sess *MongoSession) Error) {
	ms := this.Start(ctx)
	//nolint:contextcheck // End() uses the session's own stored context
	defer this.End()

	if ms.Error() != nil {
		ms.Error().LogError()
		return
	}

	err := Try(func() {
		RaiseErr(action(ms))
	})
	if err != nil {
		if rollbackErr := ms.Rollback(); rollbackErr != nil {
			rollbackErr.LogError()
		}
	} else {
		if commitErr := ms.Commit(); commitErr != nil {
			commitErr.LogError()
		}
	}
}
