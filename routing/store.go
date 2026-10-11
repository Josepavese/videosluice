package routing

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/Josepavese/videosluice/config"
)

var (
	ErrNoChange          = errors.New("routing plan selections are unchanged")
	ErrNoRollback        = errors.New("no previous routing plan is available")
	ErrStaleRevision     = errors.New("routing plan revision is stale")
	routingDirectorySync = func(directory *os.File) error { return directory.Sync() }
)

const routingPlanFilename = "routing-plan.json"

// Store owns the persistent operator plan outside the measured application
// release. Begin holds both an in-process mutex and an advisory process lock
// until Commit or Abort, so a caller can perform prepare/cutover under one CAS
// transaction without racing another local controller.
type Store struct {
	directory string
	mu        sync.Mutex
}

type Transaction struct {
	store         *Store
	lockFile      *os.File
	currentPlan   Plan
	currentConfig config.Config
	plan          Plan
	config        config.Config
	activated     bool
	closed        bool
}

func NewStore(directory string) (*Store, error) {
	directory = filepath.Clean(directory)
	if !filepath.IsAbs(directory) || directory == "/" {
		return nil, errors.New("routing store directory must be absolute and non-root")
	}
	return &Store{directory: directory}, nil
}

// NewStoreForPlan binds runtime mutation to the one canonical current plan
// filename. A daemon must not load one file and later persist a different
// default pathname from the same directory.
func NewStoreForPlan(path string) (*Store, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" || filepath.Base(path) != routingPlanFilename {
		return nil, fmt.Errorf("routing plan path must end with %s", routingPlanFilename)
	}
	return NewStore(filepath.Dir(path))
}

func (store *Store) Read(catalog Catalog) (Plan, error) {
	if store == nil {
		return Plan{}, errors.New("nil routing store")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	lock, err := store.lock()
	if err != nil {
		return Plan{}, err
	}
	defer unlockFile(lock)
	return store.readLocked(catalog)
}

// ReadAndReconcileCatalog is the explicit daemon-start boundary for a trusted
// central catalog update. A current-schema operator plan keeps its opaque
// selections only when every active and rollback option remains valid in the
// new catalog. Reconciliation is one atomic plan write: it advances the CAS
// revision and every stream generation because a restarted process cannot
// safely prove which media definitions changed without trusting stale catalog
// data. Ordinary reads and live ApplyGraph calls never perform this mutation.
func (store *Store) ReadAndReconcileCatalog(catalog Catalog) (Plan, bool, error) {
	if store == nil {
		return Plan{}, false, errors.New("nil routing store")
	}
	normalized, err := NormalizeCatalog(catalog)
	if err != nil {
		return Plan{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	lock, err := store.lock()
	if err != nil {
		return Plan{}, false, err
	}
	defer unlockFile(lock)
	path := filepath.Join(store.directory, routingPlanFilename)
	current, err := loadOwnedPlan(path)
	if errors.Is(err, os.ErrNotExist) {
		plan, defaultErr := DefaultPlan(normalized)
		return plan, false, defaultErr
	}
	if err != nil {
		return Plan{}, false, err
	}
	if current.CatalogRevision == normalized.Revision {
		if _, err := Materialize(normalized, current); err != nil {
			return Plan{}, false, fmt.Errorf("persisted routing plan is invalid: %w", err)
		}
		return current, false, nil
	}
	reconciled, err := reconcilePlanCatalog(normalized, current)
	if err != nil {
		return Plan{}, false, err
	}
	activated, err := store.writePlanLocked(reconciled)
	if err != nil {
		return Plan{}, activated, fmt.Errorf("persist reconciled routing plan: %w", err)
	}
	return reconciled, true, nil
}

func reconcilePlanCatalog(catalog Catalog, current Plan) (Plan, error) {
	if current.Revision == ^uint64(0) {
		return Plan{}, errors.New("routing plan revision is exhausted during catalog reconciliation")
	}
	reconciled := current
	reconciled.CatalogRevision = catalog.Revision
	reconciled.Revision++
	reconciled.Selections = cloneSelections(current.Selections)
	reconciled.StreamGenerations = cloneStreamGenerations(current.StreamGenerations)
	reconciled.RollbackSelections = cloneSelections(current.RollbackSelections)
	reconciled.SourceSelections = cloneSelections(current.SourceSelections)
	reconciled.RollbackSourceSelections = cloneSelections(current.RollbackSourceSelections)
	for streamID := range catalog.Streams {
		generation, exists := reconciled.StreamGenerations[streamID]
		if !exists {
			return Plan{}, fmt.Errorf(
				"routing catalog reconciliation has no generation for stream %q", streamID)
		}
		if generation == ^uint64(0) {
			return Plan{}, fmt.Errorf(
				"routing stream %q generation is exhausted during catalog reconciliation", streamID)
		}
		reconciled.StreamGenerations[streamID] = generation + 1
	}
	active := reconciled
	active.RollbackSelections = nil
	if _, err := Materialize(catalog, active); err != nil {
		return Plan{}, fmt.Errorf(
			"routing catalog revoked the active operator plan: %w", err)
	}
	if len(reconciled.RollbackSelections) > 0 {
		rollback := reconciled
		rollback.Selections = cloneSelections(reconciled.RollbackSelections)
		rollback.SourceSelections = cloneSelections(reconciled.RollbackSourceSelections)
		rollback.RollbackSelections = nil
		if _, err := Materialize(catalog, rollback); err != nil {
			return Plan{}, fmt.Errorf(
				"routing catalog revoked the operator rollback plan: %w", err)
		}
	}
	return reconciled, nil
}

func (store *Store) Begin(catalog Catalog, expectedRevision uint64, selections map[string]string, sourceSelections ...map[string]string) (*Transaction, error) {
	if store == nil {
		return nil, errors.New("nil routing store")
	}
	store.mu.Lock()
	lock, err := store.lock()
	if err != nil {
		store.mu.Unlock()
		return nil, err
	}
	current, err := store.readLocked(catalog)
	if err != nil {
		unlockFile(lock)
		store.mu.Unlock()
		return nil, err
	}
	modes := current.SourceSelections
	if len(sourceSelections) > 1 {
		return nil, store.failBegin(lock, errors.New("source selections must be unique"))
	}
	if len(sourceSelections) == 1 && sourceSelections[0] != nil {
		modes = sourceSelections[0]
	}
	return store.beginLocked(catalog, lock, current, expectedRevision, selections, modes)
}

// BeginRollback prepares the last replaced selection set as a new monotonic
// transaction. Reusing the old revision or stream generations would make late
// frames from the failed/superseded generation indistinguishable, so rollback
// always derives a fresh plan from the active state.
func (store *Store) BeginRollback(catalog Catalog, expectedRevision uint64) (*Transaction, error) {
	if store == nil {
		return nil, errors.New("nil routing store")
	}
	store.mu.Lock()
	lock, err := store.lock()
	if err != nil {
		store.mu.Unlock()
		return nil, err
	}
	current, err := store.readLocked(catalog)
	if err != nil {
		return nil, store.failBegin(lock, err)
	}
	if current.Revision != expectedRevision {
		return nil, store.failBegin(lock, fmt.Errorf("%w: expected=%d current=%d", ErrStaleRevision, expectedRevision, current.Revision))
	}
	if len(current.RollbackSelections) == 0 || (selectionsEqual(current.Selections, current.RollbackSelections) && selectionsEqual(current.SourceSelections, current.RollbackSourceSelections)) {
		return nil, store.failBegin(lock, ErrNoRollback)
	}
	return store.beginLocked(catalog, lock, current, expectedRevision, current.RollbackSelections, current.RollbackSourceSelections)
}

func (store *Store) beginLocked(
	catalog Catalog,
	lock *os.File,
	current Plan,
	expectedRevision uint64,
	selections map[string]string,
	sourceSelections map[string]string,
) (*Transaction, error) {
	if len(sourceSelections) == 0 {
		sourceSelections = nil
	}
	if current.Revision != expectedRevision {
		return nil, store.failBegin(lock, fmt.Errorf("%w: expected=%d current=%d", ErrStaleRevision, expectedRevision, current.Revision))
	}
	if selectionsEqual(current.Selections, selections) && selectionsEqual(current.SourceSelections, sourceSelections) {
		return nil, store.failBegin(lock, ErrNoChange)
	}
	if current.Revision == ^uint64(0) {
		return nil, store.failBegin(lock, errors.New("routing plan revision is exhausted"))
	}
	currentConfig, err := Materialize(catalog, current)
	if err != nil {
		return nil, store.failBegin(lock, fmt.Errorf("materialize current routing plan: %w", err))
	}
	candidate := Plan{
		Schema: PlanSchema, CatalogRevision: current.CatalogRevision,
		Revision: current.Revision + 1, Selections: cloneSelections(selections),
		StreamGenerations:        cloneStreamGenerations(current.StreamGenerations),
		RollbackSelections:       cloneSelections(current.Selections),
		SourceSelections:         cloneSelections(sourceSelections),
		RollbackSourceSelections: cloneSelections(current.SourceSelections),
	}
	materialized, err := Materialize(catalog, candidate)
	if err != nil {
		return nil, store.failBegin(lock, err)
	}
	delta, err := DiffConfigs(currentConfig, materialized)
	if err != nil {
		return nil, store.failBegin(lock, err)
	}
	if !delta.Changed && selectionsEqual(current.SourceSelections, sourceSelections) {
		return nil, store.failBegin(lock, ErrNoChange)
	}
	for _, streamID := range delta.ChangedStreamIDs {
		if candidate.StreamGenerations[streamID] == ^uint64(0) {
			return nil, store.failBegin(lock, fmt.Errorf("routing stream %q generation is exhausted", streamID))
		}
		candidate.StreamGenerations[streamID]++
	}
	return &Transaction{
		store: store, lockFile: lock, currentPlan: current, currentConfig: currentConfig,
		plan: candidate, config: materialized,
	}, nil
}

func (store *Store) failBegin(lock *os.File, err error) error {
	unlockFile(lock)
	store.mu.Unlock()
	return err
}

func (transaction *Transaction) CurrentPlan() Plan {
	if transaction == nil {
		return Plan{}
	}
	plan := transaction.currentPlan
	plan.Selections = cloneSelections(transaction.currentPlan.Selections)
	plan.StreamGenerations = cloneStreamGenerations(transaction.currentPlan.StreamGenerations)
	plan.RollbackSelections = cloneSelections(transaction.currentPlan.RollbackSelections)
	plan.SourceSelections = cloneSelections(transaction.currentPlan.SourceSelections)
	plan.RollbackSourceSelections = cloneSelections(transaction.currentPlan.RollbackSourceSelections)
	return plan
}

func (transaction *Transaction) CurrentConfig() config.Config {
	if transaction == nil {
		return config.Config{}
	}
	return cloneConfig(transaction.currentConfig)
}

func (transaction *Transaction) Plan() Plan {
	if transaction == nil {
		return Plan{}
	}
	plan := transaction.plan
	plan.Selections = cloneSelections(transaction.plan.Selections)
	plan.StreamGenerations = cloneStreamGenerations(transaction.plan.StreamGenerations)
	plan.RollbackSelections = cloneSelections(transaction.plan.RollbackSelections)
	plan.SourceSelections = cloneSelections(transaction.plan.SourceSelections)
	plan.RollbackSourceSelections = cloneSelections(transaction.plan.RollbackSourceSelections)
	return plan
}

func (transaction *Transaction) Config() config.Config {
	if transaction == nil {
		return config.Config{}
	}
	return cloneConfig(transaction.config)
}

func (transaction *Transaction) Commit() error {
	if transaction == nil || transaction.store == nil || transaction.closed {
		return errors.New("routing transaction is closed")
	}
	err := transaction.activate()
	transaction.close()
	return err
}

func (transaction *Transaction) activate() error {
	activated, err := transaction.store.writePlanLocked(transaction.plan)
	transaction.activated = activated
	return err
}

// Activated reports whether the atomic rename made the candidate plan visible.
// It remains meaningful after Commit returns an error, allowing the runtime
// coordinator to avoid rolling back to a graph that disagrees with the file
// already published on disk.
func (transaction *Transaction) Activated() bool {
	return transaction != nil && transaction.activated
}

func (transaction *Transaction) Abort() error {
	if transaction == nil || transaction.store == nil || transaction.closed {
		return errors.New("routing transaction is closed")
	}
	transaction.close()
	return nil
}

func (transaction *Transaction) close() {
	transaction.closed = true
	unlockFile(transaction.lockFile)
	transaction.lockFile = nil
	transaction.store.mu.Unlock()
}

func (store *Store) lock() (*os.File, error) {
	if err := ensurePrivateDirectory(store.directory); err != nil {
		return nil, err
	}
	path := filepath.Join(store.directory, "routing-plan.lock")
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open routing store lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("open routing store lock returned no file")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect routing store lock: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !ownedByEffectiveUser(info) {
		_ = file.Close()
		return nil, errors.New("routing store lock metadata is unsafe")
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock routing store: %w", err)
	}
	return file, nil
}

func unlockFile(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("create routing store directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect routing store directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !ownedByEffectiveUser(info) {
		return errors.New("routing store directory metadata is unsafe")
	}
	return nil
}

func (store *Store) readLocked(catalog Catalog) (Plan, error) {
	path := filepath.Join(store.directory, routingPlanFilename)
	plan, err := loadOwnedPlan(path)
	if errors.Is(err, os.ErrNotExist) {
		return DefaultPlan(catalog)
	}
	if err != nil {
		return Plan{}, err
	}
	if _, err := Materialize(catalog, plan); err != nil {
		return Plan{}, fmt.Errorf("persisted routing plan is invalid: %w", err)
	}
	return plan, nil
}

func loadOwnedPlan(path string) (Plan, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return Plan{}, fmt.Errorf("open routing plan: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return Plan{}, errors.New("open routing plan returned no file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Plan{}, fmt.Errorf("inspect routing plan: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !ownedByEffectiveUser(info) {
		return Plan{}, errors.New("routing plan metadata is unsafe")
	}
	var plan Plan
	if err := decodeStrict(file, &plan); err != nil {
		return Plan{}, fmt.Errorf("decode routing plan: %w", err)
	}
	if err := ValidatePlanEnvelope(plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func ownedByEffectiveUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func (store *Store) writePlanLocked(plan Plan) (bool, error) {
	body, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return false, fmt.Errorf("encode routing plan: %w", err)
	}
	body = append(body, '\n')
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return false, fmt.Errorf("create routing plan nonce: %w", err)
	}
	temporary := filepath.Join(store.directory, ".routing-plan."+hex.EncodeToString(nonce)+".tmp")
	target := filepath.Join(store.directory, routingPlanFilename)
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return false, fmt.Errorf("create routing plan temporary: %w", err)
	}
	cleanup := true
	defer func() {
		_ = file.Close()
		if cleanup {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := file.Write(body); err != nil {
		return false, fmt.Errorf("write routing plan: %w", err)
	}
	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("sync routing plan: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close routing plan: %w", err)
	}
	if err := os.Rename(temporary, target); err != nil {
		return false, fmt.Errorf("activate routing plan: %w", err)
	}
	cleanup = false
	directory, err := os.Open(store.directory)
	if err != nil {
		return true, fmt.Errorf("open routing store for sync: %w", err)
	}
	defer directory.Close()
	if err := routingDirectorySync(directory); err != nil {
		return true, fmt.Errorf("sync routing store: %w", err)
	}
	return true, nil
}

func selectionsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for streamID, optionID := range left {
		if right[streamID] != optionID {
			return false
		}
	}
	return true
}

func cloneConfig(input config.Config) config.Config {
	return config.Clone(input)
}
