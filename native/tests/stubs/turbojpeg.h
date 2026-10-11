#ifndef VIDEO_BROKER_TEST_TURBOJPEG_H
#define VIDEO_BROKER_TEST_TURBOJPEG_H

#include <stddef.h>

typedef void *tjhandle;

#define TJSAMP_GRAY 3
#define TJFLAG_FASTDCT 2048

tjhandle tjInitDecompress(void);
int tjDecompressHeader3(tjhandle, const unsigned char *, unsigned long, int *, int *, int *, int *);
int tjDecompressToYUVPlanes(tjhandle, const unsigned char *, unsigned long, unsigned char **, int, int *, int, int);
int tjPlaneWidth(int, int, int);
int tjPlaneHeight(int, int, int);
const char *tjGetErrorStr2(tjhandle);
int tjDestroy(tjhandle);

#endif
