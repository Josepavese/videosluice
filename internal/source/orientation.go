package source

const (
	VideoOrientationNormal           = "normal"
	VideoOrientationRotate180        = "rotate_180"
	VideoOrientationMirrorHorizontal = "mirror_horizontal"
	VideoOrientationMirrorVertical   = "mirror_vertical"
)

func NormalizeVideoOrientation(value string) string {
	switch value {
	case "normal":
		return VideoOrientationNormal
	case "rotate_180":
		return VideoOrientationRotate180
	case "mirror_horizontal":
		return VideoOrientationMirrorHorizontal
	case "mirror_vertical":
		return VideoOrientationMirrorVertical
	default:
		return ""
	}
}

func normalizeOrientationForCommand(value string) string {
	orientation := NormalizeVideoOrientation(value)
	if orientation == "" {
		return VideoOrientationNormal
	}
	return orientation
}
