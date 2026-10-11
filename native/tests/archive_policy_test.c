#include <assert.h>
#include <string.h>

#include "../archive_policy.h"

int main(void) {
	assert(video_broker_parse_archive_content(NULL) == VIDEO_BROKER_ARCHIVE_ADAPTIVE);
	assert(video_broker_parse_archive_content("adaptive") == VIDEO_BROKER_ARCHIVE_ADAPTIVE);
	assert(video_broker_parse_archive_content("SOURCE") == VIDEO_BROKER_ARCHIVE_SOURCE);
	assert(video_broker_parse_archive_content("processed") == VIDEO_BROKER_ARCHIVE_PROCESSED);

	assert(video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_ADAPTIVE, true, false));
	assert(!video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_ADAPTIVE, true, true));
	assert(video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_SOURCE, true, true));
	assert(!video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_PROCESSED, true, false));
	assert(!video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_ADAPTIVE, false, false));
	assert(!video_broker_archive_uses_native(VIDEO_BROKER_ARCHIVE_SOURCE, false, true));

	assert(strcmp(video_broker_archive_content_name(VIDEO_BROKER_ARCHIVE_SOURCE), "source") == 0);
	assert(strcmp(video_broker_archive_content_name(VIDEO_BROKER_ARCHIVE_PROCESSED), "processed") == 0);
	return 0;
}
