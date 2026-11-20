package tx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/timandy/routine"
	"go.mongodb.org/mongo-driver/mongo"

	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
)

// MongoSession represents an active MongoDB session and transaction context,
// or an ambient background context when no transaction is active.
// It implements [context.Context] so it can be passed directly to MongoDB driver calls.
type MongoSession struct {
	//nolint:containedctx // Wraps mongo.SessionContext for transaction propagation
	ctx           context.Context
	session       mongo.Session
	err           Error
	committed     bool
	aborted       bool
	ended         bool
	commitPending bool
}

var _ context.Context = &MongoSession{}

// NewMongoSession creates a [MongoSession] bound to the given context and optional [mongo.Session].
func NewMongoSession(ctx context.Context, session mongo.Session) *MongoSession {
	//nolint:contextcheck // Fallback to background context when caller passes nil
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if session != nil {
		baseCtx = mongo.NewSessionContext(baseCtx, session)
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
	if this.ended {
		return NewError("TransactionError", "Session has already ended")
	}
	if this.err != nil {
		return this.err
	}
	if this.commitPending {
		return NewError("TransactionError", "Transaction commit outcome is unknown and pending retry")
	}
	if this.aborted {
		return NewError("TransactionError", "Transaction has already been rolled back")
	}
	if this.committed {
		return NewError("TransactionError", "Transaction has already been committed")
	}
	return nil
}

func isUnknownTransactionCommitResult(err error) bool {
	if err == nil {
		return false
	}
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && cmdErr.HasErrorLabel("UnknownTransactionCommitResult") {
		return true
	}
	if labeled, ok := err.(interface{ HasErrorLabel(string) bool }); ok && labeled.HasErrorLabel("UnknownTransactionCommitResult") {
		return true
	}
	return strings.Contains(err.Error(), "UnknownTransactionCommitResult")
}

func isServerSelectionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "server selection")
}

// Commit commits the active MongoDB transaction on this session.
func (this *MongoSession) Commit() Error {
	if this == nil {
		return NewError("TransactionError", "No active session")
	}
	if this.ended {
		return NewError("TransactionError", "Session has already ended")
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
	err := this.session.CommitTransaction(this.ctx)
	if err != nil && isUnknownTransactionCommitResult(err) && this.ctx.Err() != nil {
		retryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if retryErr := this.session.CommitTransaction(mongo.NewSessionContext(retryCtx, this.session)); retryErr != nil {
			if !isServerSelectionError(retryErr) {
				err = retryErr
			}
		} else {
			err = nil
		}
	}
	if err != nil {
		wrapped := NewWrappedError("TransactionError", err)
		if isUnknownTransactionCommitResult(err) {
			this.commitPending = true
			return wrapped
		}
		this.commitPending = false
		this.err = wrapped
		return this.err
	}
	this.commitPending = false
	this.committed = true
	return nil
}

// Rollback aborts the active MongoDB transaction on this session.
func (this *MongoSession) Rollback() Error {
	if this == nil {
		return NewError("TransactionError", "No active session")
	}
	if this.ended {
		return NewError("TransactionError", "Session has already ended")
	}
	if this.session == nil {
		if this.err != nil {
			return this.err
		}
		return NewError("TransactionError", "No active MongoDB session to rollback")
	}
	if this.commitPending {
		return NewError("TransactionError", "Cannot rollback transaction with pending commit result")
	}
	if this.committed || this.aborted {
		return NewError("TransactionError", "Transaction already finalized")
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := this.session.AbortTransaction(mongo.NewSessionContext(cleanupCtx, this.session)); err != nil {
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

// End closes the underlying [mongo.Session] if one is open and marks this session wrapper as ended.
func (this *MongoSession) End() {
	if this != nil {
		this.ended = true
		if this.session != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			this.session.EndSession(mongo.NewSessionContext(cleanupCtx, this.session))
		}
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
	if key == (*MongoSession)(nil) {
		return this
	}
	return this.Context().Value(key)
}

// MongoTransactional represents the interface of a [Transactional] manager for [MongoSession].
type MongoTransactional interface {
	Transactional[MongoSession]
}

// MongoTransactionalManager manages goroutine-bound MongoDB sessions and transactions.
// It has no Rays Stereotype so multiple instances can be instantiated or registered via Configuration.
type MongoTransactionalManager struct {
	client   *mongo.Client
	logger   Logging
	sessions map[uint64][]*MongoSession
	mu       sync.Mutex
}

var _ MongoTransactional = &MongoTransactionalManager{}

// NewMongoTransactionalManager creates a new [MongoTransactionalManager] for the given [*mongo.Client] and [Logging].
func NewMongoTransactionalManager(client *mongo.Client, logger Logging) *MongoTransactionalManager {
	return &MongoTransactionalManager{
		client:   client,
		logger:   logger,
		sessions: make(map[uint64][]*MongoSession),
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

	ms := NewMongoSession(baseCtx, sess)
	goid := routine.Goid()
	this.mu.Lock()
	this.sessions[goid] = append(this.sessions[goid], ms)
	this.mu.Unlock()
	return ms
}

// Get returns the active [MongoSession] for the current goroutine, or an ambient background session.
func (this *MongoTransactionalManager) Get() *MongoSession {
	goid := routine.Goid()
	this.mu.Lock()
	stack := this.sessions[goid]
	var ms *MongoSession
	if len(stack) > 0 {
		ms = stack[len(stack)-1]
	}
	this.mu.Unlock()
	if ms != nil {
		return ms
	}
	return NewMongoSession(context.Background(), nil)
}

// End closes any active session on the current goroutine and clears the session binding.
func (this *MongoTransactionalManager) End() {
	if this.logger != nil {
		this.logger.Trace("Ending Transaction", "manager", "MongoTransactionalManager")
	}
	goid := routine.Goid()
	var ms *MongoSession
	this.mu.Lock()
	stack := this.sessions[goid]
	if len(stack) > 0 {
		ms = stack[len(stack)-1]
		if len(stack) == 1 {
			delete(this.sessions, goid)
		} else {
			this.sessions[goid] = stack[:len(stack)-1]
		}
	}
	this.mu.Unlock()

	if ms != nil {
		ms.End()
	}
}

// Transactional executes action inside a MongoDB transaction, automatically
// committing on nil Error or rolling back on any returned Error or panic.
//
//nolint:contextcheck // Rollback and End use bounded cleanup contexts independent of request cancellation
func (this *MongoTransactionalManager) Transactional(ctx context.Context, action func(sess *MongoSession) Error) {
	ms := this.Start(ctx)
	if ms.Error() != nil {
		ms.Error().LogError()
		return
	}
	//nolint:contextcheck // End() uses bounded cleanup context
	defer this.End()

	var actionErr Error
	panicked := true
	defer func() {
		if panicked {
			//nolint:errcheck // Best-effort rollback on panic
			_ = ms.Rollback()
		}
	}()

	actionErr = action(ms)
	panicked = false

	if actionErr != nil {
		if rollbackErr := ms.Rollback(); rollbackErr != nil {
			rollbackErr.LogError()
		}
		return
	}

	if commitErr := ms.Commit(); commitErr != nil {
		commitErr.LogError()
	}
}

// Execute executes action inside a MongoDB transaction, returning the startup, callback, or commit error.
// If action returns an error and rollback also fails, the original callback error is propagated.
//
//nolint:contextcheck // Rollback and End use bounded cleanup contexts independent of request cancellation
func (this *MongoTransactionalManager) Execute(ctx context.Context, action func(sess *MongoSession) Error) (retErr Error) {
	ms := this.Start(ctx)
	if ms.Error() != nil {
		return ms.Error()
	}
	//nolint:contextcheck // End() uses bounded cleanup context
	defer this.End()

	panicked := true
	defer func() {
		if panicked {
			//nolint:errcheck // Best-effort rollback on panic
			_ = ms.Rollback()
		}
		if r := recover(); r != nil {
			retErr = NewError("TransactionPanicError", fmt.Sprintf("Transaction callback panicked: %v", r))
		}
	}()

	actionErr := action(ms)
	panicked = false

	if actionErr != nil {
		if rollbackErr := ms.Rollback(); rollbackErr != nil && this.logger != nil {
			this.logger.Error("Rollback failed after action error", "error", rollbackErr, "actionError", actionErr)
		}
		return actionErr
	}

	return ms.Commit()
}

// ExecuteWithResult executes action inside a MongoDB transaction and returns the action result along with any error.
// If action returns an error and rollback also fails, the original callback error is propagated.
//
//nolint:contextcheck // Rollback and End use bounded cleanup contexts independent of request cancellation
func ExecuteWithResult[T any](ctx context.Context, mgr *MongoTransactionalManager, action func(sess *MongoSession) (T, Error)) (_ T, retErr Error) {
	var zero T
	if mgr == nil {
		return zero, NewError("TransactionError", "No transaction manager provided")
	}

	ms := mgr.Start(ctx)
	if ms.Error() != nil {
		return zero, ms.Error()
	}
	//nolint:contextcheck // End() uses bounded cleanup context
	defer mgr.End()

	panicked := true
	defer func() {
		if panicked {
			//nolint:errcheck // Best-effort rollback on panic
			_ = ms.Rollback()
		}
		if r := recover(); r != nil {
			retErr = NewError("TransactionPanicError", fmt.Sprintf("Transaction callback panicked: %v", r))
		}
	}()

	result, actionErr := action(ms)
	panicked = false

	if actionErr != nil {
		if rollbackErr := ms.Rollback(); rollbackErr != nil && mgr.logger != nil {
			mgr.logger.Error("Rollback failed after action error", "error", rollbackErr, "actionError", actionErr)
		}
		return zero, actionErr
	}

	if commitErr := ms.Commit(); commitErr != nil {
		return zero, commitErr
	}

	return result, nil
}
