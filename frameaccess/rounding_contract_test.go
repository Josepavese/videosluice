package frameaccess_test

import (
	fa "github.com/Josepavese/videosluice/frameaccess"
	"testing"
)

// The SDK independently rounds each binary64 operation. ARM64 FMA contraction
// must not change the final even-pixel ROI and retire an otherwise valid lease.
func TestMapRegionBinary64PaddingBoundary(t *testing.T) {
	tests := []struct {
		name                    string
		width, height, rotation int
		mirror                  bool
		want                    fa.Rect
	}{
		{"identity", 640, 360, 0, false, fa.Rect{X1: 2, Y1: 2, X2: 90, Y2: 118}},
		{"double", 1280, 720, 0, false, fa.Rect{X1: 4, Y1: 4, X2: 178, Y2: 236}},
		{"native-hd", 1920, 1080, 0, false, fa.Rect{X1: 6, Y1: 6, X2: 268, Y2: 354}},
		{"mirror", 1920, 1080, 0, true, fa.Rect{X1: 1652, Y1: 6, X2: 1914, Y2: 354}},
		{"rotate90", 1080, 1920, 90, false, fa.Rect{X1: 6, Y1: 1652, X2: 354, Y2: 1914}},
		{"rotate180", 1920, 1080, 180, false, fa.Rect{X1: 1652, Y1: 726, X2: 1914, Y2: 1074}},
		{"rotate270-mirror", 1080, 1920, 270, true, fa.Rect{X1: 726, Y1: 1652, X2: 1074, Y2: 1914}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transform := fa.Transform{Full: fa.Geometry{Width: test.width, Height: test.height, Stride: test.width},
				Crop: fa.Rect{X2: test.width, Y2: test.height}, Rotation: test.rotation, Mirror: test.mirror,
				ResizedWidth: 640, ResizedHeight: 360, DeliveryWidth: 640, DeliveryHeight: 360}
			got, err := fa.MapRegion(transform, fa.Region{ID: "body", BBox: [4]float64{8, 10, 83, 110}, PaddingRatio: .08})
			if err != nil || got != test.want {
				t.Fatalf("ROI differs from binary64 SDK contract: got=%+v want=%+v err=%v", got, test.want, err)
			}
		})
	}
}
