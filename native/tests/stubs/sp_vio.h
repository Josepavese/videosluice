#ifndef VIDEO_BROKER_TEST_SP_VIO_H
#define VIDEO_BROKER_TEST_SP_VIO_H

#define FRAME_BUFFER_SIZE(width, height) (((width) * (height) * 3) / 2)

typedef struct {
	int raw_width;
	int raw_height;
	int fps;
} sp_sensors_parameters;

void *sp_init_vio_module(void);
void sp_release_vio_module(void *);
int sp_open_camera_v2(void *, int, int, int, sp_sensors_parameters *, int *, int *);
int sp_vio_get_frame(void *, char *, int, int, int);
int sp_vio_close(void *);

#endif
