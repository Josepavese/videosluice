#ifndef VIDEO_BROKER_TEST_HB_MEDIA_CODEC_H
#define VIDEO_BROKER_TEST_HB_MEDIA_CODEC_H

#include <stdbool.h>
#include <stdint.h>

typedef int32_t hb_s32;
typedef uint32_t hb_u32;
typedef uint64_t hb_u64;
typedef uint8_t hb_u8;
typedef bool hb_bool;
typedef void *hb_ptr;

typedef enum {
	MEDIA_CODEC_ID_NONE = 0,
	MEDIA_CODEC_ID_H264 = 1,
	MEDIA_CODEC_ID_H265 = 2,
	MEDIA_CODEC_ID_MJPEG = 3,
} media_codec_id_t;

typedef enum {
	MC_PIXEL_FORMAT_YUV420P = 0,
	MC_PIXEL_FORMAT_NV12 = 1,
	MC_PIXEL_FORMAT_NV21 = 2,
} mc_pixel_format_t;

typedef enum {
	MC_AV_RC_MODE_NONE = -1,
	MC_AV_RC_MODE_H264CBR = 0,
	MC_AV_RC_MODE_H264AVBR = 2,
	MC_AV_RC_MODE_H265CBR = 5,
	MC_AV_RC_MODE_H265AVBR = 7,
} mc_video_rate_control_mode_t;

typedef enum {
	MC_VIDEO_FRAME_BUFFER = 1,
	MC_VIDEO_STREAM_BUFFER = 2,
} media_codec_buffer_type_t;

typedef enum {
	MC_CCW_0 = 0,
} mc_rotate_degree_t;

typedef enum {
	MC_DIRECTION_NONE = 0,
} mc_mirror_direction_t;

typedef enum {
	MC_FEEDING_MODE_NONE = -1,
	MC_FEEDING_MODE_STREAM_SIZE,
	MC_FEEDING_MODE_FRAME_SIZE,
} mc_av_stream_feeding_mode_t;

typedef struct {
	hb_u32 intra_period;
	hb_u32 intra_qp;
	hb_u32 bit_rate;
	hb_u32 frame_rate;
	hb_u32 initial_rc_qp;
	hb_s32 vbv_buffer_size;
	hb_bool mb_level_rc_enalbe;
	hb_u32 min_qp_I;
	hb_u32 max_qp_I;
	hb_u32 min_qp_P;
	hb_u32 max_qp_P;
	hb_u32 min_qp_B;
	hb_u32 max_qp_B;
	hb_bool hvs_qp_enable;
	hb_s32 hvs_qp_scale;
	hb_u32 max_delta_qp;
	hb_bool qp_map_enable;
} mc_h264_cbr_params_t;

typedef mc_h264_cbr_params_t mc_h264_avbr_params_t;

typedef struct {
	hb_u32 intra_period;
	hb_u32 intra_qp;
	hb_u32 bit_rate;
	hb_u32 frame_rate;
	hb_u32 initial_rc_qp;
	hb_s32 vbv_buffer_size;
	hb_bool ctu_level_rc_enalbe;
	hb_u32 min_qp_I;
	hb_u32 max_qp_I;
	hb_u32 min_qp_P;
	hb_u32 max_qp_P;
	hb_u32 min_qp_B;
	hb_u32 max_qp_B;
	hb_bool hvs_qp_enable;
	hb_s32 hvs_qp_scale;
	hb_u32 max_delta_qp;
	hb_bool qp_map_enable;
} mc_h265_cbr_params_t;

typedef mc_h265_cbr_params_t mc_h265_avbr_params_t;

typedef struct {
	mc_video_rate_control_mode_t mode;
	union {
		mc_h264_cbr_params_t h264_cbr_params;
		mc_h264_avbr_params_t h264_avbr_params;
		mc_h265_cbr_params_t h265_cbr_params;
		mc_h265_avbr_params_t h265_avbr_params;
	};
} mc_rate_control_params_t;

typedef struct {
	hb_s32 gop_preset_idx;
	hb_s32 decoding_refresh_type;
} mc_video_gop_params_t;

typedef struct {
	hb_bool reorder_enable;
} mc_h264_dec_config_t;

typedef struct {
	hb_bool reorder_enable;
} mc_h265_dec_config_t;

typedef struct {
	hb_s32 width;
	hb_s32 height;
	mc_pixel_format_t pix_fmt;
	hb_u32 frame_buf_count;
	hb_bool external_frame_buf;
	hb_u32 bitstream_buf_count;
	hb_u32 bitstream_buf_size;
	mc_rate_control_params_t rc_params;
	mc_video_gop_params_t gop_params;
	mc_rotate_degree_t rot_degree;
	mc_mirror_direction_t mir_direction;
	hb_bool frame_cropping_flag;
	hb_bool enable_user_pts;
} mc_video_codec_enc_params_t;

typedef struct {
	mc_av_stream_feeding_mode_t feed_mode;
	mc_pixel_format_t pix_fmt;
	hb_u32 bitstream_buf_size;
	hb_u32 bitstream_buf_count;
	hb_bool external_bitstream_buf;
	hb_u32 frame_buf_count;
	union {
		mc_h264_dec_config_t h264_dec_config;
		mc_h265_dec_config_t h265_dec_config;
	};
} mc_video_codec_dec_params_t;

typedef struct {
	media_codec_id_t codec_id;
	hb_bool encoder;
	hb_s32 instance_index;
	union {
		mc_video_codec_enc_params_t video_enc_params;
		mc_video_codec_dec_params_t video_dec_params;
	};
	hb_ptr test_private;
} media_codec_context_t;

typedef struct {
	hb_s32 reserved;
} mc_av_codec_startup_params_t;

typedef struct {
	hb_u8 *vir_ptr[3];
	hb_u32 size;
	hb_u32 compSize[3];
	hb_s32 width;
	hb_s32 height;
	mc_pixel_format_t pix_fmt;
	hb_s32 stride;
	hb_s32 vstride;
	hb_u64 pts;
} mc_video_frame_buffer_info_t;

typedef struct {
	hb_u8 *vir_ptr;
	hb_u32 size;
	hb_s32 stream_end;
	hb_u64 pts;
} mc_video_stream_buffer_info_t;

typedef struct {
	media_codec_buffer_type_t type;
	union {
		mc_video_frame_buffer_info_t vframe_buf;
		mc_video_stream_buffer_info_t vstream_buf;
	};
} media_codec_buffer_t;

typedef struct {
	union {
		struct {
			hb_s32 decode_result;
		} video_frame_info;
		struct {
			hb_s32 decode_result;
		} jpeg_frame_info;
	};
} media_codec_output_buffer_info_t;

hb_s32 hb_mm_mc_get_default_context(
	media_codec_id_t codec_id,
	hb_bool encoder,
	media_codec_context_t *context);
hb_s32 hb_mm_mc_initialize(media_codec_context_t *context);
hb_s32 hb_mm_mc_configure(media_codec_context_t *context);
hb_s32 hb_mm_mc_start(
	media_codec_context_t *context,
	mc_av_codec_startup_params_t *params);
hb_s32 hb_mm_mc_pause(media_codec_context_t *context);
hb_s32 hb_mm_mc_stop(media_codec_context_t *context);
hb_s32 hb_mm_mc_release(media_codec_context_t *context);
hb_s32 hb_mm_mc_get_rate_control_config(
	media_codec_context_t *context,
	mc_rate_control_params_t *params);
hb_s32 hb_mm_mc_set_rate_control_config(
	media_codec_context_t *context,
	const mc_rate_control_params_t *params);
hb_s32 hb_mm_mc_request_idr_frame(media_codec_context_t *context);
hb_s32 hb_mm_mc_request_idr_header(
	media_codec_context_t *context,
	hb_u32 force_header);
hb_s32 hb_mm_mc_get_explicit_header_config(
	media_codec_context_t *context,
	hb_s32 *status);
hb_s32 hb_mm_mc_set_explicit_header_config(
	media_codec_context_t *context,
	hb_s32 status);
hb_s32 hb_mm_mc_dequeue_input_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout);
hb_s32 hb_mm_mc_queue_input_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout);
hb_s32 hb_mm_mc_dequeue_output_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	media_codec_output_buffer_info_t *info,
	hb_s32 timeout);
hb_s32 hb_mm_mc_queue_output_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout);

#endif
