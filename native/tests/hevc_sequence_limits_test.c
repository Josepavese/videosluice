#include "../hevc_sequence_limits.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct { unsigned char bytes[512]; unsigned int bits; } bit_writer;
static void put(bit_writer *out, uint32_t value, unsigned int count) {
	assert(out->bits + count <= sizeof(out->bytes) * 8);
	for (unsigned int n = count; n > 0; --n, out->bits++)
		out->bytes[out->bits / 8] |= ((value >> (n - 1)) & 1U) << (7 - out->bits % 8);
}
static void ue(bit_writer *out, uint32_t value) {
	uint32_t code = value + 1;
	unsigned int count = 0;
	for (uint32_t n = code; n > 1; n >>= 1) count++;
	put(out, 0, count); put(out, code, count + 1);
}
static size_t sps(unsigned char out[1024], uint32_t width, uint32_t height,
	uint32_t crop_bottom, uint32_t depth, uint32_t dpb, unsigned int sublayers) {
	bit_writer writer = {0};
	put(&writer, 0, 4); put(&writer, sublayers, 3); put(&writer, 1, 1);
	put(&writer, 1U << 24, 32); put(&writer, 0, 32); put(&writer, 0, 24); put(&writer, 120, 8);
	for (unsigned int i = 0; i < sublayers; ++i) { put(&writer, 1, 1); put(&writer, 1, 1); }
	if (sublayers) put(&writer, 0, (8 - sublayers) * 2);
	for (unsigned int i = 0; i < sublayers; ++i) {
		put(&writer, 0, 32); put(&writer, 0, 32); put(&writer, 0, 24); put(&writer, 120, 8);
	}
	ue(&writer, 0); ue(&writer, 1); ue(&writer, width); ue(&writer, height);
	put(&writer, crop_bottom != 0, 1);
	if (crop_bottom) { ue(&writer, 0); ue(&writer, 0); ue(&writer, 0); ue(&writer, crop_bottom); }
	ue(&writer, depth); ue(&writer, depth); ue(&writer, 4); put(&writer, 1, 1);
	for (unsigned int i = 0; i <= sublayers; ++i) { ue(&writer, dpb - 1); ue(&writer, dpb - 1); ue(&writer, 0); }
	put(&writer, 1, 1);
	size_t size = 0; unsigned int zeros = 0;
	out[size++] = 0; out[size++] = 0; out[size++] = 0; out[size++] = 1;
	out[size++] = 33U << 1; out[size++] = 1;
	for (unsigned int i = 0; i < (writer.bits + 7) / 8; ++i) {
		unsigned char byte = writer.bytes[i];
		if (zeros == 2 && byte <= 3) { out[size++] = 3; zeros = 0; }
		out[size++] = byte;
		zeros = byte == 0 ? zeros + 1 : 0;
	}
	return size;
}

int main(int argc, char **argv) {
	if (argc == 4) {
		FILE *file = fopen(argv[1], "rb"); assert(file != NULL);
		unsigned char *bytes = malloc(8U << 20); assert(bytes != NULL);
		size_t size = fread(bytes, 1, 8U << 20, file);
		assert(size > 0 && fgetc(file) == EOF); fclose(file);
		vb_hevc_sequence_guard guard = {0};
		int result = vb_hevc_guard_access_unit(&guard, bytes, size, (uint32_t)atoi(argv[2]), (uint32_t)atoi(argv[3]));
		printf("fixture=%s result=%d coded=%ux%u dpb=%u reorder=%u\n", argv[1], result,
			guard.sequence.coded_width, guard.sequence.coded_height, guard.sequence.dpb_frames, guard.sequence.reorder_frames);
		free(bytes);
		return result == 1 && guard.seen_sequence ? 0 : 1;
	}
	assert(argc == 1);
	unsigned char bytes[1024];
	for (unsigned int layers = 0; layers <= 6; ++layers) {
		for (uint32_t dpb = 1; dpb <= 16; ++dpb) {
			vb_hevc_sequence_guard guard = {0};
			size_t size = sps(bytes, 1920, 1088, 4, 0, dpb, layers);
			assert(vb_hevc_guard_access_unit(&guard, bytes, size, 1920, 1080) == 1);
			assert(guard.seen_sequence && guard.sequence.dpb_frames == dpb && guard.sequence.coded_height == 1088);
			for (size_t cut = 0; cut + 1 < size; ++cut) {
				vb_hevc_sequence_guard truncated = {0};
				if (vb_hevc_guard_access_unit(&truncated, bytes, cut, 1920, 1080) == 1) {
					// The guard intentionally reads only the allocation prefix, not
					// all SPS syntax or rbsp_trailing_bits. It cannot accept before
					// the independently written prefix is completely present.
					assert(cut >= size - 2);
				}
			}
		}
	}
	vb_hevc_sequence_guard guard = {0};
	size_t size = sps(bytes, 1280, 720, 0, 0, 6, 0);
	assert(vb_hevc_guard_access_unit(&guard, bytes, size, 1280, 720) == 1);
	vb_hevc_sequence_guard admitted = guard;
	const uint32_t cases[][4] = {{1920,1080,0,6}, {1280,720,2,6}, {1280,720,0,17}, {1278,720,0,6}};
	for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); ++i) {
		size = sps(bytes, cases[i][0], cases[i][1], 0, cases[i][2], cases[i][3], 0);
		assert(vb_hevc_guard_access_unit(&guard, bytes, size, 1280, 720) == -1);
		assert(memcmp(&guard, &admitted, sizeof(guard)) == 0);
	}
	unsigned char early_vcl[] = {0,0,1,2,1,0x80};
	vb_hevc_sequence_guard fresh = {0};
	assert(vb_hevc_guard_access_unit(&fresh, early_vcl, sizeof(early_vcl), 1280, 720) == 0);
	assert(vb_hevc_guard_access_unit(&guard, early_vcl, sizeof(early_vcl), 1280, 720) == 1);
	early_vcl[4] = 9; /* non-base layer */
	assert(vb_hevc_guard_access_unit(&guard, early_vcl, sizeof(early_vcl), 1280, 720) == -1);
	unsigned char malformed_epb[] = {0,0,3,4};
	vb_hevc_bits bits = {.data=malformed_epb, .size=sizeof(malformed_epb)};
	(void)vb_hevc_read(&bits, 32); assert(bits.invalid);
	puts("OK_HEVC_SEQUENCE_LIMITS");
	return 0;
}
