package frameaccess

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPublicationDiagnosticDurationsAreExactAndAbsentStagesExplicit(t *testing.T) {
	point := func(ns uint64) publicationPoint { return publicationPoint{ns: ns, valid: true} }
	e := &publicationFailure{
		progress: publicationProgress{phase: "copy", started: point(100000), copyStarted: point(120000), copyEnded: point(170000), commitAcquired: point(177000)},
		ended:    point(180000), cause: DeadlineExceeded, deadline: true,
	}
	want := "surface_publish_progress phase=copy admission_us=20 copy_us=50 commit_wait_us=7 finalization_us=3 elapsed_us=80 context_canceled=false context_deadline=true: deadline_exceeded"
	if got := e.Error(); got != want || !errors.Is(e, DeadlineExceeded) {
		t.Fatalf("diagnostic timing/code differs: %s", got)
	}
	e.progress.copyStarted = publicationPoint{}
	e.progress.copyEnded = publicationPoint{}
	e.progress.commitAcquired = publicationPoint{}
	if !strings.Contains(e.Error(), "admission_us=-1 copy_us=-1 commit_wait_us=-1 finalization_us=-1") {
		t.Fatalf("missing phases were reported as completed: %s", e)
	}
	if publicationElapsed(point(0), point(1000)) != 1 || publicationElapsed(point(1000), point(0)) != -1 {
		t.Fatal("zero clock origin or regressing clock was misrepresented")
	}
}

func TestSuccessfulPublicationDiagnosticDoesNotAllocateAnError(t *testing.T) {
	p := publicationProgress{phase: "admission"}
	if count := testing.AllocsPerRun(1000, func() {
		if p.failure(context.Background(), nil) != nil {
			panic("healthy publication became an error")
		}
	}); count != 0 {
		t.Fatalf("healthy diagnostic allocated %.0f objects", count)
	}
}
