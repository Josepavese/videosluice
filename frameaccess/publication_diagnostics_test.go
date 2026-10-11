package frameaccess_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
)

type publicationFailureCopier struct{ cause error }

func (publicationFailureCopier) Capability() fa.CopyCapability {
	return fa.CopyCapability{Provider: "cpu_nv12_copy"}
}
func (c publicationFailureCopier) CopyNV12(context.Context, fa.CopyBudget, []byte, []byte, fa.Geometry, fa.Rect) error {
	return c.cause
}

func TestPublicationFailureSeparatesCopyAndCommitWithoutChangingCode(t *testing.T) {
	st, c, scope := fixture(t, publicationFailureCopier{cause: context.DeadlineExceeded})
	err := st.TryPublishSurface(context.Background(), origin(scope, 1), sourcePixels(scope.Geometry, 1), c.NowNS(), 200)
	if !errors.Is(err, fa.DeadlineExceeded) {
		t.Fatalf("publication changed the existing semantic code: %v", err)
	}
	for _, marker := range []string{"surface_publish_progress", "phase=copy", "admission_us=", "copy_us=", "commit_wait_us=", "finalization_us=", "elapsed_us="} {
		if !strings.Contains(err.Error(), marker) {
			t.Fatalf("publication failure lacks %q: %v", marker, err)
		}
	}
	if st.Stats().UsedBytes != 0 || st.Stats().CopyBusy {
		t.Fatal("failed publication retained arena bytes or the copy slot")
	}
}
