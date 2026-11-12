#include <assert.h>
#include <stdio.h>
#include "v4l2_autofocus.h"

static int value, writes, reads, queries, absent, drift, failed;
static unsigned flags;
static int fake_ioctl(int fd, unsigned long request, void *arg) {
	assert(fd == 7);
	if (request == VIDIOC_QUERYCTRL) {
		struct v4l2_queryctrl *q = arg;
		assert(q->id == V4L2_CID_FOCUS_AUTO);
		queries++;
		if (absent) {errno = EINVAL;return -1;}
		q->type = V4L2_CTRL_TYPE_BOOLEAN;q->flags=flags;return 0;
	}
	struct v4l2_control *c = arg;
	assert(c->id == V4L2_CID_FOCUS_AUTO);
	if (request == VIDIOC_G_CTRL) {reads++;c->value=value;return 0;}
	assert(request == VIDIOC_S_CTRL);writes++;assert(c->value==1);
	if(failed){errno=EIO;return -1;}
	if(!drift)value=c->value;
	return 0;
}
static void reset(void){value=writes=reads=queries=absent=drift=failed=0;flags=0;}
int main(void) {
	reset();absent=1;
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"unsupported"));assert(!reads&&!writes);
	reset();value=1;
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"already_enabled"));assert(reads==1&&!writes);
	reset();
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"enabled"));assert(reads==2&&writes==1&&queries==1);
	reset();drift=1;
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"verification_drift"));
	reset();failed=1;
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"write_failed"));assert(reads==1);
	reset();flags=V4L2_CTRL_FLAG_READ_ONLY;
	assert(!strcmp(vb_enable_continuous_focus(7,fake_ioctl),"not_writable"));assert(!writes);
	puts("V4L2_AUTOFOCUS_PASS");return 0;
}
