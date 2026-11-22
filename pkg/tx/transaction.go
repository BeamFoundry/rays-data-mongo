package tx

import (
	"context"
	"errors"
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
	coordinator   TransactionManager
	owner         *MongoTransactionalManager
	committed     bool
	aborted       bool
	ended         bool
	commitPending bool
	transactional bool
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
		ctx:           baseCtx,
		session:       session,
		transactional: session != nil,
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

// ReportError records an [Error] against this [MongoSession].
// When this session belongs to its owning manager's currently active coordinated transaction scope,
// the error is forwarded to the session's coordinating [TransactionManager]; otherwise, the error
// is recorded directly on this transactional session and the session is rolled back without
// affecting any later or nested transaction. While commitPending is true, validation errors are
// not cached so later Commit calls remain able to retry commit recovery.
func (this *MongoSession) ReportError(err Error) {
	if err == nil || this == nil || !this.transactional || this.commitPending {
		return
	}
	if this.owner != nil && this.coordinator != nil && this.owner.txManager == this.coordinator && this.coordinator.IsActive() && this.owner.currentSession() == this {
		this.coordinator.SetError(err)
		return
	}
	if this.Error() == nil {
		this.SetError(err)
	}
	//nolint:errcheck // Primary error is already recorded on the session
	_ = this.Rollback()
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
	// Database returns the [*mongo.Database] associated with this transactional manager, if configured.
	Database() *mongo.Database
	// ReportSessionError records an [Error] against the captured [MongoSession] from when a query was built.
	ReportSessionError(sess *MongoSession, err Error)
}

var (
	activeSessionsMu sync.Mutex
	activeSessions   = make(map[uint64]map[*mongo.Client][]*MongoSession)
)

// MongoTransactionalManager manages goroutine-bound MongoDB sessions and transactions.
// It has no Rays Stereotype so multiple instances can be instantiated or registered via Configuration.
type MongoTransactionalManager struct {
	txManager TransactionManager `@:"Inject"`
	logger    Logging            `@:"Inject"`
	client    *mongo.Client
	database  Optional[*mongo.Database] `@:"Inject"`
}

var _ MongoTransactional = &MongoTransactionalManager{}

// New creates a new [MongoTransactionalManager] for Prototype registration in [Configuration] Beams.
func (this MongoTransactionalManager) New() *MongoTransactionalManager {
	return &this
}

// NewMongoTransactionalManager creates a new [MongoTransactionalManager] for the given [*mongo.Client] and [Logging].
func NewMongoTransactionalManager(client *mongo.Client, logger Logging) *MongoTransactionalManager {
	return &MongoTransactionalManager{
		client: client,
		logger: logger,
	}
}

// WithDatabase sets the [*mongo.Database] on this [MongoTransactionalManager]
// and returns the receiver for fluent chaining.
func (this *MongoTransactionalManager) WithDatabase(database *mongo.Database) *MongoTransactionalManager {
	if database != nil {
		this.database = OptionalOf(database)
	} else {
		this.database = OptionalEmpty[*mongo.Database]()
	}
	return this
}

// WithTransactionManager sets the coordinating [TransactionManager] on this [MongoTransactionalManager]
// and returns the receiver for fluent chaining.
func (this *MongoTransactionalManager) WithTransactionManager(txManager TransactionManager) *MongoTransactionalManager {
	this.txManager = txManager
	return this
}

// Database returns the [*mongo.Database] bound to this [MongoTransactionalManager].
func (this *MongoTransactionalManager) Database() *mongo.Database {
	if db, ok := this.database.Get(); ok {
		return db
	}
	return nil
}

func (this *MongoTransactionalManager) resolvedClient() *mongo.Client {
	if this.client != nil {
		return this.client
	}
	if db := this.Database(); db != nil {
		return db.Client()
	}
	return nil
}

func (this *MongoTransactionalManager) activeContext() context.Context {
	if this.txManager != nil {
		return this.txManager.GetContext()
	}
	return context.Background()
}

func (this *MongoTransactionalManager) currentSession() *MongoSession {
	goid := routine.Goid()
	client := this.resolvedClient()
	activeSessionsMu.Lock()
	defer activeSessionsMu.Unlock()
	if stack := activeSessions[goid][client]; len(stack) > 0 {
		return stack[len(stack)-1]
	}
	return nil
}

func (this *MongoTransactionalManager) pushSession(ms *MongoSession) {
	goid := routine.Goid()
	client := this.resolvedClient()
	activeSessionsMu.Lock()
	defer activeSessionsMu.Unlock()
	byClient := activeSessions[goid]
	if byClient == nil {
		byClient = make(map[*mongo.Client][]*MongoSession)
		activeSessions[goid] = byClient
	}
	byClient[client] = append(byClient[client], ms)
}

func (this *MongoTransactionalManager) popSession() *MongoSession {
	goid := routine.Goid()
	client := this.resolvedClient()
	activeSessionsMu.Lock()
	defer activeSessionsMu.Unlock()
	byClient := activeSessions[goid]
	if stack := byClient[client]; len(stack) > 0 {
		ms := stack[len(stack)-1]
		if len(stack) == 1 {
			delete(byClient, client)
			if len(byClient) == 0 {
				delete(activeSessions, goid)
			}
		} else {
			byClient[client] = stack[:len(stack)-1]
		}
		return ms
	}
	return nil
}

// Start begins a new MongoDB session and transaction and binds it to the current goroutine.
func (this *MongoTransactionalManager) Start() Error {
	baseCtx := this.activeContext()
	if this.logger != nil {
		this.logger.Trace("Start Transaction", "manager", "MongoTransactionalManager")
	}

	var sess mongo.Session
	if err := Try(func() {
		sess = RaiseVal(this.resolvedClient().StartSession())
		RaiseErr(sess.StartTransaction())
	}); err != nil {
		txErr := NewWrappedError("TransactionError", err)
		ms := NewMongoSession(baseCtx, nil)
		ms.owner = this
		ms.transactional = true
		ms.SetError(txErr)
		this.pushSession(ms)
		return txErr
	}

	ms := NewMongoSession(baseCtx, sess)
	ms.owner = this
	ms.transactional = true
	this.pushSession(ms)
	return nil
}

// Commit commits the active MongoDB transaction on the current goroutine.
func (this *MongoTransactionalManager) Commit() Error {
	if this.logger != nil {
		this.logger.Trace("Commit Transaction", "manager", "MongoTransactionalManager")
	}
	if ms := this.currentSession(); ms != nil {
		return ms.Commit()
	}
	return NewError("TransactionError", "No active session")
}

// Rollback aborts the active MongoDB transaction on the current goroutine.
func (this *MongoTransactionalManager) Rollback() Error {
	if this.logger != nil {
		this.logger.Trace("Rollback Transaction", "manager", "MongoTransactionalManager")
	}
	if ms := this.currentSession(); ms != nil {
		return ms.Rollback()
	}
	return NewError("TransactionError", "No active session")
}

// Get returns the active [MongoSession] for the current goroutine, lazily enlisting in
// the [TransactionManager] if a coordinated transaction is active, or returning an
// ambient session bound to the current goroutine's context.
func (this *MongoTransactionalManager) Get() *MongoSession {
	if ms := this.currentSession(); ms != nil {
		return ms
	}
	if this.txManager != nil && this.txManager.IsActive() {
		if err := this.txManager.Enlist(this); err != nil {
			if ms := this.currentSession(); ms != nil {
				ms.coordinator = this.txManager
				return ms
			}
			ms := NewMongoSession(this.activeContext(), nil)
			ms.owner = this
			ms.coordinator = this.txManager
			ms.SetError(err)
			return ms
		}
		ms := this.currentSession()
		ms.coordinator = this.txManager
		return ms
	}
	ms := NewMongoSession(this.activeContext(), nil)
	ms.owner = this
	return ms
}

// End closes any active session on the current goroutine and clears the session binding.
func (this *MongoTransactionalManager) End() {
	if this.logger != nil {
		this.logger.Trace("Ending Transaction", "manager", "MongoTransactionalManager")
	}
	if ms := this.popSession(); ms != nil {
		ms.End()
	}
}

// SetError records an [Error] on the active transaction and triggers rollback via the
// coordinating [TransactionManager] when active (or directly on the active [MongoSession]).
func (this *MongoTransactionalManager) SetError(err Error) {
	if err == nil {
		return
	}
	if this.txManager != nil && this.txManager.IsActive() {
		this.txManager.SetError(err)
		return
	}
	if ms := this.currentSession(); ms != nil && !ms.commitPending {
		if ms.Error() == nil {
			ms.SetError(err)
		}
		//nolint:errcheck // Primary error is already recorded on the session
		_ = ms.Rollback()
	}
}

// ReportSessionError records an [Error] against the [MongoSession] bound to a query, dispatching
// through the session's owning [MongoTransactionalManager] and coordinating [TransactionManager]
// when the session belongs to that owner's currently active coordinated transaction scope, or
// rolling back the transactional session directly while preserving commitPending retry recovery.
func (this *MongoTransactionalManager) ReportSessionError(sess *MongoSession, err Error) {
	sess.ReportError(err)
}

// Error returns the [Error] recorded on the coordinating [TransactionManager] or active [MongoSession].
func (this *MongoTransactionalManager) Error() Error {
	if this.txManager != nil {
		if err := this.txManager.Error(); err != nil {
			return err
		}
	}
	if ms := this.currentSession(); ms != nil {
		return ms.Error()
	}
	return nil
}
