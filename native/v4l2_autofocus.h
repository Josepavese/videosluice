#ifndef VIDEO_BROKER_V4L2_AUTOFOCUS_H
#define VIDEO_BROKER_V4L2_AUTOFOCUS_H

#include <errno.h>
#include <linux/videodev2.h>
#include <string.h>

typedef int (*vb_focus_ioctl)(int, unsigned long, void *);

/* Source-owned pre-STREAMON transaction on the already-open capture FD.
 * QUERYCTRL is descriptor-only; do not read/write unrelated ISP controls.
 * A mode readback never claims that the optics actually reached focus. */
static inline const char *vb_enable_continuous_focus(int fd, vb_focus_ioctl call) {
	struct v4l2_queryctrl query;
	struct v4l2_control control;
	const int enabled = 1; /* V4L2 boolean true */
	memset(&query, 0, sizeof(query));
	query.id = V4L2_CID_FOCUS_AUTO;
	if (call(fd, VIDIOC_QUERYCTRL, &query) != 0)
		return errno == EINVAL ? "unsupported" : "query_failed";
	if (query.flags & V4L2_CTRL_FLAG_DISABLED)
		return "unsupported";
	if (query.type != V4L2_CTRL_TYPE_BOOLEAN)
		return "invalid_capability";
	memset(&control, 0, sizeof(control));
	control.id = V4L2_CID_FOCUS_AUTO;
	if (call(fd, VIDIOC_G_CTRL, &control) != 0)
		return "read_failed";
	if (control.value == enabled)
		return "already_enabled";
	if (query.flags & (V4L2_CTRL_FLAG_READ_ONLY | V4L2_CTRL_FLAG_INACTIVE | V4L2_CTRL_FLAG_GRABBED))
		return "not_writable";
	control.value = enabled;
	if (call(fd, VIDIOC_S_CTRL, &control) != 0)
		return "write_failed";
	if (call(fd, VIDIOC_G_CTRL, &control) != 0)
		return "verification_failed";
	return control.value == enabled ? "enabled" : "verification_drift";
}
#endif
