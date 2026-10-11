package framedomain

import (
	"errors"
	"strings"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
)

func TestResourceFailureRetainsDiagnosticCauseAndClassification(t *testing.T) {
	cause := errors.New("native execution stage=sdk_release_unconfirmed process_state=signal: killed")
	err := &ResourceFailure{Stage: "release_failed", Cause: cause}
	if !strings.Contains(err.Error(), cause.Error()) || !errors.Is(err, cause) || !errors.Is(err, fa.ProviderUnavailable) {
		t.Fatal("resource failure hides its cause or classification", err)
	}
	if got := (&ResourceFailure{Stage: "release_failed"}).Error(); got != "resource lifecycle not confirmed: stage=release_failed" {
		t.Fatal(got)
	}
}
