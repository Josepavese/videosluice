package source

import "testing"

func TestNormalizeVideoOrientationAcceptsOnlyCanonicalValues(t *testing.T) {
	canonical := []string{
		VideoOrientationNormal,
		VideoOrientationRotate180,
		VideoOrientationMirrorHorizontal,
		VideoOrientationMirrorVertical,
	}
	for _, value := range canonical {
		if got := NormalizeVideoOrientation(value); got != value {
			t.Fatalf("canonical orientation %q resolved to %q", value, got)
		}
	}
	for _, removedAlias := range []string{"", "none", "180", "rotate-180", "upside-down", "hflip", "vflip", " normal "} {
		if got := NormalizeVideoOrientation(removedAlias); got != "" {
			t.Fatalf("removed orientation alias %q resolved to %q", removedAlias, got)
		}
	}
}
