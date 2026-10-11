package routing

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNewStoreForPlanRequiresCanonicalCurrentPath(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "routing")
	store, err := NewStoreForPlan(filepath.Join(directory, routingPlanFilename))
	if err != nil || store.directory != directory {
		t.Fatalf("canonical plan path returned store=%+v err=%v", store, err)
	}
	for _, path := range []string{
		"routing-plan.json",
		filepath.Join(directory, "plan.json"),
		"/",
	} {
		if _, err := NewStoreForPlan(path); err == nil {
			t.Fatalf("non-canonical plan path accepted: %q", path)
		}
	}
}

func TestStoreCommitsAtomicCASPlanAndPersistsAcrossOpen(t *testing.T) {
	catalog := testCatalog(2)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Revision != 1 || initial.Selections["analysis"] != "analysis_a" ||
		initial.StreamGenerations["live"] != 1 || initial.StreamGenerations["analysis"] != 1 ||
		initial.StreamGenerations["archive"] != 1 {
		t.Fatalf("unexpected default plan: %+v", initial)
	}
	transaction, err := store.Begin(catalog, 1, map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if transaction.Plan().Revision != 2 || transaction.Config().Streams["analysis"].SourceRef != "camera_b" ||
		transaction.Plan().StreamGenerations["analysis"] != 2 ||
		transaction.Plan().StreamGenerations["live"] != 1 ||
		transaction.Plan().StreamGenerations["archive"] != 1 {
		t.Fatalf("unexpected prepared transaction: plan=%+v config=%+v", transaction.Plan(), transaction.Config())
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Revision != 2 || persisted.Selections["analysis"] != "analysis_b" ||
		persisted.StreamGenerations["analysis"] != 2 ||
		persisted.StreamGenerations["live"] != 1 || persisted.StreamGenerations["archive"] != 1 ||
		!reflect.DeepEqual(persisted.RollbackSelections, catalog.DefaultSelections) {
		t.Fatalf("unexpected persisted plan: %+v", persisted)
	}
	info, err := os.Stat(filepath.Join(store.directory, "routing-plan.json"))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 ||
		!ownedByEffectiveUser(info) {
		t.Fatalf("unsafe plan metadata info=%v err=%v", info, err)
	}
	if _, err := store.Begin(catalog, 1, persisted.Selections); !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale CAS was accepted: %v", err)
	}
	if _, err := store.Begin(catalog, 2, persisted.Selections); !errors.Is(err, ErrNoChange) {
		t.Fatalf("unchanged plan advanced its revision: %v", err)
	}
}

func TestStoreRollbackCreatesFreshMonotonicPlanAndTogglesHistory(t *testing.T) {
	catalog := testCatalog(2)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	forwardSelections := map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	}
	forward, err := store.Begin(catalog, 1, forwardSelections)
	if err != nil {
		t.Fatal(err)
	}
	forwardView := forward.Plan()
	if !reflect.DeepEqual(forwardView.RollbackSelections, catalog.DefaultSelections) {
		t.Fatalf("forward rollback target=%v want=%v", forwardView.RollbackSelections, catalog.DefaultSelections)
	}
	forwardView.RollbackSelections["analysis"] = "analysis_b"
	if forward.Plan().RollbackSelections["analysis"] != "analysis_a" {
		t.Fatal("transaction rollback target was mutable through returned plan")
	}
	if err := forward.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginRollback(catalog, 1); !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale rollback CAS was accepted: %v", err)
	}

	rollback, err := store.BeginRollback(catalog, 2)
	if err != nil {
		t.Fatal(err)
	}
	rollbackPlan := rollback.Plan()
	wantGenerations := map[string]uint64{"live": 1, "analysis": 3, "archive": 1}
	if rollbackPlan.Revision != 3 ||
		!reflect.DeepEqual(rollbackPlan.Selections, catalog.DefaultSelections) ||
		!reflect.DeepEqual(rollbackPlan.RollbackSelections, forwardSelections) ||
		!reflect.DeepEqual(rollbackPlan.StreamGenerations, wantGenerations) {
		t.Fatalf("unexpected rollback candidate: %+v", rollbackPlan)
	}
	if err := rollback.Commit(); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, rollbackPlan) {
		t.Fatalf("persisted rollback=%+v want=%+v", persisted, rollbackPlan)
	}

	toggle, err := store.BeginRollback(catalog, 3)
	if err != nil {
		t.Fatal(err)
	}
	togglePlan := toggle.Plan()
	if togglePlan.Revision != 4 || togglePlan.Selections["analysis"] != "analysis_b" ||
		togglePlan.RollbackSelections["analysis"] != "analysis_a" ||
		togglePlan.StreamGenerations["analysis"] != 4 ||
		togglePlan.StreamGenerations["live"] != 1 || togglePlan.StreamGenerations["archive"] != 1 {
		t.Fatalf("rollback history did not toggle monotonically: %+v", togglePlan)
	}
	if err := toggle.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRollbackRequiresHistoryAfterCASValidation(t *testing.T) {
	catalog := testCatalog(1)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginRollback(catalog, 0); !errors.Is(err, ErrStaleRevision) {
		t.Fatalf("stale revision was hidden by missing history: %v", err)
	}
	if _, err := store.BeginRollback(catalog, 1); !errors.Is(err, ErrNoRollback) {
		t.Fatalf("rollback without history was accepted: %v", err)
	}
}

func TestStoreRejectsInvalidPersistedRollbackSelections(t *testing.T) {
	catalog := testCatalog(2)
	for name, rollbackSelections := range map[string]map[string]string{
		"missing stream": {"live": "live_a", "analysis": "analysis_a"},
		"unknown option": {"live": "live_a", "analysis": "analysis_a", "archive": "raw_device"},
	} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "routing")
			if err := os.Mkdir(directory, 0o750); err != nil {
				t.Fatal(err)
			}
			plan, err := DefaultPlan(catalog)
			if err != nil {
				t.Fatal(err)
			}
			plan.RollbackSelections = rollbackSelections
			body, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, routingPlanFilename), body, 0o640); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Read(catalog); err == nil || !strings.Contains(err.Error(), "rollback selections") {
				t.Fatalf("invalid rollback target was accepted: %v", err)
			}
		})
	}
}

func TestPreparedConfigCannotBeMutatedThroughTransactionView(t *testing.T) {
	catalog := testCatalog(2)
	front := catalog.Sources["camera_a"]
	front.Controls = map[string]int{"exposure_absolute": 17}
	catalog.Sources["camera_a"] = front
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Begin(catalog, 1, map[string]string{
		"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transaction.Abort()
	view := transaction.Config()
	front = view.Sources["camera_a"]
	front.Device = "/dev/unvalidated-override"
	front.Controls["exposure_absolute"] = 1
	view.Sources["camera_a"] = front
	delete(view.Streams, "analysis")
	fresh := transaction.Config()
	if fresh.Sources["camera_a"].Device == "/dev/unvalidated-override" ||
		fresh.Sources["camera_a"].Controls["exposure_absolute"] != 17 ||
		fresh.Streams["analysis"].Type == "" {
		t.Fatalf("transaction config was mutable through returned view: %+v", fresh)
	}
	current := transaction.CurrentConfig()
	front = current.Sources["camera_a"]
	front.Controls["exposure_absolute"] = 2
	current.Sources["camera_a"] = front
	currentPlan := transaction.CurrentPlan()
	currentPlan.Selections["analysis"] = "analysis_b"
	currentPlan.StreamGenerations["analysis"] = 99
	if transaction.CurrentConfig().Sources["camera_a"].Controls["exposure_absolute"] != 17 ||
		transaction.CurrentPlan().Selections["analysis"] != "analysis_a" ||
		transaction.CurrentPlan().StreamGenerations["analysis"] != 1 {
		t.Fatal("current transaction view was mutable through returned values")
	}
}

func TestStoreRejectsExhaustedChangedStreamGeneration(t *testing.T) {
	catalog := testCatalog(2)
	directory := filepath.Join(t.TempDir(), "routing")
	if err := os.Mkdir(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	plan, err := DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	plan.StreamGenerations["analysis"] = ^uint64(0)
	body, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "routing-plan.json"), body, 0o640); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Begin(catalog, 1, map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	})
	if err == nil || !strings.Contains(err.Error(), "generation is exhausted") {
		t.Fatalf("exhausted stream generation was accepted: %v", err)
	}
}

func TestStoreReconcilesTrustedCatalogAtomicallyAndOnlyOnce(t *testing.T) {
	oldCatalog := testCatalog(2)
	directory := filepath.Join(t.TempDir(), "routing")
	store, err := NewStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Begin(oldCatalog, 1, map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	before := transaction.Plan()
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}

	newCatalog := testCatalog(2)
	newCatalog.Revision = "sha256:" + strings.Repeat("b", 64)
	front := newCatalog.Sources["camera_a"]
	front.FPS = 25
	newCatalog.Sources["camera_a"] = front
	live := newCatalog.Streams["live"]
	option := live.Options["live_a"]
	option.FPS = 25
	live.Options["live_a"] = option
	newCatalog.Streams["live"] = live

	reconciled, changed, err := store.ReadAndReconcileCatalog(newCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || reconciled.Revision != before.Revision+1 ||
		reconciled.CatalogRevision != newCatalog.Revision ||
		!reflect.DeepEqual(reconciled.Selections, before.Selections) ||
		!reflect.DeepEqual(reconciled.RollbackSelections, before.RollbackSelections) {
		t.Fatalf("unexpected reconciled plan: before=%+v after=%+v", before, reconciled)
	}
	for streamID, generation := range before.StreamGenerations {
		if reconciled.StreamGenerations[streamID] != generation+1 {
			t.Fatalf("stream %q generation=%d want=%d", streamID, reconciled.StreamGenerations[streamID], generation+1)
		}
	}
	materialized, err := Materialize(newCatalog, reconciled)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Sources["camera_a"].FPS != 25 || materialized.Streams["live"].FPS != 25 {
		t.Fatalf("reconciled plan did not materialize the new central definition: %+v", materialized)
	}
	second, changed, err := store.ReadAndReconcileCatalog(newCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if changed || !reflect.DeepEqual(second, reconciled) {
		t.Fatalf("same catalog reconciled more than once: first=%+v second=%+v changed=%t", reconciled, second, changed)
	}
	persisted, err := store.Read(newCatalog)
	if err != nil || !reflect.DeepEqual(persisted, reconciled) {
		t.Fatalf("reconciled plan was not persisted atomically: plan=%+v err=%v", persisted, err)
	}
}

func TestStoreCatalogReconciliationFailsClosedOnRevokedChoices(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		prepare          func(*testing.T, *Store, Catalog) Plan
		wantError        string
		removeLiveOption string
	}{
		{
			name: "active choice", removeLiveOption: "live_b", wantError: "revoked the active operator plan",
			prepare: func(t *testing.T, store *Store, catalog Catalog) Plan {
				transaction, err := store.Begin(catalog, 1, map[string]string{
					"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
				})
				if err != nil {
					t.Fatal(err)
				}
				plan := transaction.Plan()
				if err := transaction.Commit(); err != nil {
					t.Fatal(err)
				}
				return plan
			},
		},
		{
			name: "rollback choice", removeLiveOption: "live_b", wantError: "revoked the operator rollback plan",
			prepare: func(t *testing.T, store *Store, catalog Catalog) Plan {
				forward, err := store.Begin(catalog, 1, map[string]string{
					"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := forward.Commit(); err != nil {
					t.Fatal(err)
				}
				rollback, err := store.BeginRollback(catalog, 2)
				if err != nil {
					t.Fatal(err)
				}
				plan := rollback.Plan()
				if err := rollback.Commit(); err != nil {
					t.Fatal(err)
				}
				return plan
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			oldCatalog := testCatalog(2)
			directory := filepath.Join(t.TempDir(), "routing")
			store, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			before := testCase.prepare(t, store, oldCatalog)
			path := filepath.Join(directory, routingPlanFilename)
			beforeBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			newCatalog := testCatalog(2)
			newCatalog.Revision = "sha256:" + strings.Repeat("b", 64)
			live := newCatalog.Streams["live"]
			delete(live.Options, testCase.removeLiveOption)
			newCatalog.Streams["live"] = live
			_, changed, err := store.ReadAndReconcileCatalog(newCatalog)
			if err == nil || changed || !strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("revoked choice was not rejected causally: changed=%t err=%v", changed, err)
			}
			afterBytes, readErr := os.ReadFile(path)
			if readErr != nil || !reflect.DeepEqual(afterBytes, beforeBytes) {
				t.Fatalf("rejected reconciliation changed persisted bytes: err=%v", readErr)
			}
			persisted, readErr := store.Read(oldCatalog)
			if readErr != nil || !reflect.DeepEqual(persisted, before) {
				t.Fatalf("rejected reconciliation changed old plan: plan=%+v err=%v", persisted, readErr)
			}
		})
	}
}

func TestStoreCatalogReconciliationRejectsMonotonicCounterExhaustion(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*Plan)
		want   string
	}{
		{"plan revision", func(plan *Plan) { plan.Revision = ^uint64(0) }, "revision is exhausted"},
		{"stream generation", func(plan *Plan) { plan.StreamGenerations["analysis"] = ^uint64(0) }, "generation is exhausted"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			oldCatalog := testCatalog(2)
			directory := filepath.Join(t.TempDir(), "routing")
			if err := os.Mkdir(directory, 0o750); err != nil {
				t.Fatal(err)
			}
			plan, err := DefaultPlan(oldCatalog)
			if err != nil {
				t.Fatal(err)
			}
			testCase.mutate(&plan)
			body, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, routingPlanFilename)
			if err := os.WriteFile(path, body, 0o640); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			newCatalog := testCatalog(2)
			newCatalog.Revision = "sha256:" + strings.Repeat("b", 64)
			_, changed, err := store.ReadAndReconcileCatalog(newCatalog)
			if err == nil || changed || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("exhausted counter accepted: changed=%t err=%v", changed, err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !reflect.DeepEqual(after, body) {
				t.Fatalf("exhausted reconciliation changed plan bytes: err=%v", readErr)
			}
		})
	}
}

func TestStoreCatalogReconciliationReturnsUnpersistedDefaultForMissingPlan(t *testing.T) {
	catalog := testCatalog(1)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	plan, changed, err := store.ReadAndReconcileCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if changed || plan.Revision != 1 || plan.CatalogRevision != catalog.Revision {
		t.Fatalf("unexpected missing-plan result: plan=%+v changed=%t", plan, changed)
	}
	if _, err := os.Stat(filepath.Join(store.directory, routingPlanFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default plan was persisted without an operator change: %v", err)
	}
}

func TestStoreAdvancesOnlyChangedGenerationsAcrossSequentialCommits(t *testing.T) {
	catalog := testCatalog(2)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(catalog, 1, map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(catalog, 2, map[string]string{
		"live": "live_b", "analysis": "analysis_b", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
	third, err := store.Begin(catalog, 3, map[string]string{
		"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Commit(); err != nil {
		t.Fatal(err)
	}
	plan, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{"live": 2, "analysis": 3, "archive": 1}
	if plan.Revision != 4 || !reflect.DeepEqual(plan.StreamGenerations, want) {
		t.Fatalf("sequential generations=%+v revision=%d want=%+v revision=4", plan.StreamGenerations, plan.Revision, want)
	}
}

func TestStoreAbortLeavesDefaultAndReleasesConcurrentController(t *testing.T) {
	catalog := testCatalog(2)
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Begin(catalog, 1, map[string]string{
		"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		transaction *Transaction
		err         error
	}
	ready := make(chan result, 1)
	go func() {
		transaction, beginErr := store.Begin(catalog, 1, map[string]string{
			"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
		})
		ready <- result{transaction: transaction, err: beginErr}
	}()
	select {
	case <-ready:
		t.Fatal("concurrent controller bypassed the active transaction lock")
	case <-time.After(30 * time.Millisecond):
	}
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-ready:
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		if err := outcome.transaction.Abort(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent controller did not resume after abort")
	}
	if _, err := os.Stat(filepath.Join(store.directory, "routing-plan.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aborted transaction persisted a plan: %v", err)
	}
}

func TestStoreProcessCrashBeforeCommitKeepsPersistedPlanAndReleasesLock(t *testing.T) {
	for _, phase := range []string{"prepare", "cutover"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "routing")
			catalog := testCatalog(2)
			catalogPath := filepath.Join(root, "catalog.json")
			body, err := json.Marshal(catalog)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(catalogPath, append(body, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			baselineTransaction, err := store.Begin(catalog, 1, map[string]string{
				"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
			})
			if err != nil {
				t.Fatal(err)
			}
			baseline := baselineTransaction.Plan()
			if err := baselineTransaction.Commit(); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestStoreProcessCrashHelper$")
			command.Env = append(os.Environ(),
				"VIDEO_BROKER_STORE_CRASH_HELPER=1",
				"VIDEO_BROKER_STORE_CRASH_PHASE="+phase,
				"VIDEO_BROKER_STORE_CRASH_DIRECTORY="+directory,
				"VIDEO_BROKER_STORE_CRASH_CATALOG="+catalogPath,
			)
			output, runErr := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != 86 {
				t.Fatalf("%s crash helper exit=%v output=%s", phase, runErr, output)
			}

			reopened, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := reopened.Read(catalog)
			if err != nil {
				t.Fatalf("read after %s crash: %v", phase, err)
			}
			if !reflect.DeepEqual(persisted, baseline) {
				t.Fatalf("%s crash advanced the persisted plan: got=%+v want=%+v", phase, persisted, baseline)
			}
			recovered, err := reopened.Begin(catalog, baseline.Revision, map[string]string{
				"live": "live_a", "analysis": "analysis_a", "archive": "archive_a",
			})
			if err != nil {
				t.Fatalf("kernel did not release routing lock after %s crash: %v", phase, err)
			}
			if recovered.Plan().Revision != baseline.Revision+1 ||
				recovered.Plan().StreamGenerations["analysis"] != baseline.StreamGenerations["analysis"]+1 {
				t.Fatalf("recovered transaction is not monotonic after %s crash: %+v", phase, recovered.Plan())
			}
			if err := recovered.Abort(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreProcessCrashHelper(t *testing.T) {
	if os.Getenv("VIDEO_BROKER_STORE_CRASH_HELPER") != "1" {
		return
	}
	phase := os.Getenv("VIDEO_BROKER_STORE_CRASH_PHASE")
	if phase != "prepare" && phase != "cutover" {
		t.Fatalf("unknown crash phase %q", phase)
	}
	catalog, err := LoadCatalog(os.Getenv("VIDEO_BROKER_STORE_CRASH_CATALOG"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(os.Getenv("VIDEO_BROKER_STORE_CRASH_DIRECTORY"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := store.Begin(catalog, current.Revision, map[string]string{
		"live": "live_a", "analysis": "analysis_a", "archive": "archive_a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if transaction.Plan().Revision != current.Revision+1 {
		t.Fatalf("%s candidate revision did not advance", phase)
	}
	// ApplyGraph persists only after runtime verification. Abrupt termination at
	// either boundary must therefore leave the current plan and advisory lock to
	// be recovered by the next daemon process.
	os.Exit(86)
}

func TestStoreRejectsSymlinkDirectoryAndRevokedCatalog(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "routing")
	if err := os.Symlink(realDirectory, symlink); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(symlink)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(testCatalog(1)); err == nil {
		t.Fatal("symlink routing store accepted")
	}

	store, err = NewStore(filepath.Join(root, "safe"))
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog(2)
	tx, err := store.Begin(catalog, 1, map[string]string{"live": "live_b", "analysis": "analysis_a", "archive": "archive_a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	revoked := testCatalog(2)
	revoked.Revision = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := store.Read(revoked); err == nil {
		t.Fatal("plan from revoked catalog revision accepted")
	}
}

func TestStoreRejectsSymlinkAndWritablePersistedPlans(t *testing.T) {
	catalog := testCatalog(1)
	root := t.TempDir()

	for name, arrange := range map[string]func(string) error{
		"symlink": func(directory string) error {
			external := filepath.Join(root, "external-plan.json")
			if err := writeTestPlan(external, catalog); err != nil {
				return err
			}
			return os.Symlink(external, filepath.Join(directory, "routing-plan.json"))
		},
		"group writable": func(directory string) error {
			path := filepath.Join(directory, "routing-plan.json")
			if err := writeTestPlan(path, catalog); err != nil {
				return err
			}
			return os.Chmod(path, 0o660)
		},
	} {
		t.Run(name, func(t *testing.T) {
			directory := filepath.Join(root, strings.ReplaceAll(name, " ", "-"))
			if err := os.Mkdir(directory, 0o750); err != nil {
				t.Fatal(err)
			}
			if err := arrange(directory); err != nil {
				t.Fatal(err)
			}
			store, err := NewStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Read(catalog); err == nil {
				t.Fatal("unsafe persisted plan accepted")
			}
		})
	}
}

func TestStoreRejectsWritableLockFile(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "routing")
	if err := os.Mkdir(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(directory, "routing-plan.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Chmod is deliberate: process umask may be stricter than 0027 in build
	// sandboxes, but this fixture must still materialize an actually unsafe
	// group-writable inode.
	if err := os.Chmod(lockPath, 0o660); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(testCatalog(1)); err == nil {
		t.Fatal("unsafe lock file accepted")
	}
}

func writeTestPlan(path string, catalog Catalog) error {
	plan, err := DefaultPlan(catalog)
	if err != nil {
		return err
	}
	body, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o640)
}
