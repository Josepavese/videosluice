#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "jpeg_frame_header.h"

/* Structural fixture only: table/entropy correctness belongs to the decoder. */
static const unsigned char baseline[] = {
	0xff,0xd8, 0xff,0xe1,0,6,0xff,0xc0,0xff,0xd8,
	0xff,0xc0,0,17,8,2,208,5,0,3,1,0x22,0,2,0x11,1,3,0x11,1,
	0xff,0xda,0,12,3,1,0,2,0x11,3,0x11,0,63,0,
	0x12,0xff,0,0x34,0xff,0xd0,0x45,0xff,0xd9
};

static void rejected(const unsigned char *p, size_t size) {
	video_broker_jpeg_frame_header result;
	memset(&result,0xa5,sizeof result);
	assert(!video_broker_jpeg_frame_inspect(p,size,&result));
	assert(result.width==0 && result.height==0 && result.components==0);
}

static void structural(void) {
	video_broker_jpeg_frame_header result;
	assert(video_broker_jpeg_frame_inspect(baseline,sizeof baseline,&result));
	assert(result.width==1280 && result.height==720 && result.precision==8);
	assert(result.components==3 && result.horizontal[0]==2 && result.vertical[0]==2);
	assert(result.coding_marker==0xc0);
	assert(!video_broker_jpeg_frame_inspect(baseline,sizeof baseline,NULL));
	rejected(NULL,0);
	for(size_t n=0;n<sizeof baseline;n++) rejected(baseline,n);
	unsigned char b[256];
	/* Marker-looking bytes in APP metadata are not frame definitions. */
	memcpy(b,baseline,sizeof baseline);
	memset(b+sizeof baseline,0,3);
	b[sizeof baseline+1]=0xff;
	assert(video_broker_jpeg_frame_inspect(b,sizeof baseline+3,&result));
	memcpy(b+sizeof baseline,baseline,sizeof baseline);
	rejected(b,2*sizeof baseline); /* Never silently admit two images. */
	const unsigned offsets[]={0,1,5,13,14,19,21,24,27,30,32,33,34};
	for(size_t i=0;i<sizeof offsets/sizeof offsets[0];i++) {
		memcpy(b,baseline,sizeof baseline);
		b[offsets[i]]=0;
		rejected(b,sizeof baseline);
	}
	/* A valid but different geometry must be reported exactly, not replaced
	 * with the requested camera dimensions. The provider rejects the mismatch. */
	memcpy(b,baseline,sizeof baseline);b[18]=1;
	assert(video_broker_jpeg_frame_inspect(b,sizeof baseline,&result));
	assert(result.width==1281 && result.height==720);
	memcpy(b,baseline,sizeof baseline);b[13]=0xff;rejected(b,sizeof baseline);
	memcpy(b,baseline,sizeof baseline);b[23]=1;rejected(b,sizeof baseline); /* Duplicate component. */
	memcpy(b,baseline,sizeof baseline);b[24]=0;rejected(b,sizeof baseline); /* Sampling zero. */
	memcpy(b,baseline,sizeof baseline);b[36]=9;rejected(b,sizeof baseline); /* Unknown scan component. */
	memcpy(b,baseline,29);memcpy(b+29,baseline+10,19);
	memcpy(b+48,baseline+29,sizeof baseline-29);rejected(b,sizeof baseline+19);
	/* Multiple scans of a single SOF (e.g. progressive JPEG) keep the same
	 * geometry. Their hardware support is a separate provider capability. */
	memcpy(b,baseline,sizeof baseline-2);
	memcpy(b+sizeof baseline-2,baseline+29,sizeof baseline-29);
	b[11]=0xc2;
	assert(video_broker_jpeg_frame_inspect(b,2*sizeof baseline-31,&result));
	assert(result.coding_marker==0xc2 && result.width==1280);
	for(unsigned marker=0xdc;marker<=0xdf;marker++) {
		if(marker==0xdd)continue;
		memcpy(b,baseline,sizeof baseline);b[11]=(unsigned char)marker;
		rejected(b,sizeof baseline);
	}
	/* Bounded malformed-input sweep, with sanitizer checking every read. */
	uint32_t seed=1;
	for(unsigned sample=0;sample<20000;sample++) {
		memcpy(b,baseline,sizeof baseline);
		seed=seed*1664525U+1013904223U;
		size_t size=seed%sizeof b;
		memset(b+sizeof baseline,0,sizeof b-sizeof baseline);
		for(unsigned j=0;j<4;j++) {
			seed=seed*1664525U+1013904223U;
			b[(seed>>8)%sizeof b]=(unsigned char)seed;
		}
		(void)video_broker_jpeg_frame_inspect(b,size,&result);
	}
}

int main(int argc,char **argv) {
	structural();
	for(int i=1;i<argc;i++) {
		FILE *f=fopen(argv[i],"rb");assert(f);
		assert(fseek(f,0,SEEK_END)==0);long n=ftell(f);assert(n>0 && n<=16*1024*1024);
		assert(fseek(f,0,SEEK_SET)==0);unsigned char *p=malloc((size_t)n);assert(p);
		assert(fread(p,1,(size_t)n,f)==(size_t)n);assert(fclose(f)==0);
		video_broker_jpeg_frame_header result;
		assert(video_broker_jpeg_frame_inspect(p,(size_t)n,&result));
		assert(result.width==1280 && result.height==720 && result.precision==8);
		printf("JPEG_REAL_HEADER_PASS file=%s geometry=%ux%u components=%u sampling=%ux%u\n",
			argv[i],(unsigned)result.width,(unsigned)result.height,(unsigned)result.components,
			(unsigned)result.horizontal[0],(unsigned)result.vertical[0]);
		free(p);
	}
	puts("JPEG_ENVELOPE_GEOMETRY_PASS");
	return 0;
}
