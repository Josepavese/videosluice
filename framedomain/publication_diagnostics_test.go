package framedomain_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
)

type publicationFailureCopier struct{}

func (publicationFailureCopier) Capability() fa.CopyCapability {
	return fa.CopyCapability{Provider: "cpu_nv12_copy"}
}
func (publicationFailureCopier) CopyNV12(context.Context, fa.CopyBudget, []byte, []byte, fa.Geometry, fa.Rect) error {
	return fa.DeadlineExceeded
}

func TestDomainPublicationFailureSeparatesAdmissionStoreAndFinalization(t *testing.T) {
	r, _ := fixture(t)
	defer r.Revoke()
	b := open(t, r, "front", "analysis", publicationFailureCopier{})
	err := b.PublishSurface(context.Background(), observed(b, 1), make([]byte, 96), 100, 200)
	if !errors.Is(err, fa.DeadlineExceeded) {
		t.Fatalf("domain lost the semantic store failure: %v", err)
	}
	for _, marker := range []string{"domain_surface_publish_progress", "phase=store", "begin_us=", "store_us=", "current_us=", "finish_us=", "context_deadline=", "surface_publish_progress"} {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("publication failure lacks %q: %v", marker, err)
		}
	}
}
