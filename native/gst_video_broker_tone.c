#include <gst/base/gstbasetransform.h>
#include <gst/gst.h>
#include <gst/video/video.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "nv12_quality_measure.h"
#include "nv12_tone_mapper_accelerator.h"
#include "worker_release.h"

static _Noreturn void gst_tone_release_unconfirmed(const char *stage) {
	g_printerr("[tone-accelerator][release] stage=%s release=unconfirmed"
		" action=retain_buffers_and_retire_worker\n", stage);
	/* No GObject/atexit cleanup or core dump may release uncertain storage. */
	_Exit(VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
}

#ifndef PACKAGE
#define PACKAGE "video-broker"
#endif

typedef struct _GstVideoBrokerTone {
	GstBaseTransform parent;
	GstVideoInfo info;
	gchar *mode;
	gchar *accelerator;
	gint target_luma;
	gint hysteresis;
	gint grid_columns;
	gint grid_rows;
	gint statistics_hz;
	gint sample_stride;
	gdouble max_shadow_gain;
	gdouble max_highlight_reduction;
	gdouble temporal_alpha;
	gboolean noise_guard;
	video_broker_nv12_tone_mapper_t mapper;
	video_broker_tone_accelerator_t tone_accelerator;
	gboolean mapper_ready;
	guint8 *scratch;
	gsize scratch_size;
	gint64 last_telemetry_us;
} GstVideoBrokerTone;

typedef struct _GstVideoBrokerToneClass {
	GstBaseTransformClass parent_class;
} GstVideoBrokerToneClass;

G_DEFINE_TYPE(GstVideoBrokerTone, gst_video_broker_tone, GST_TYPE_BASE_TRANSFORM)

enum {
	PROP_0,
	PROP_MODE,
	PROP_ACCELERATOR,
	PROP_TARGET_LUMA,
	PROP_HYSTERESIS,
	PROP_GRID_COLUMNS,
	PROP_GRID_ROWS,
	PROP_STATISTICS_HZ,
	PROP_SAMPLE_STRIDE,
	PROP_MAX_SHADOW_GAIN,
	PROP_MAX_HIGHLIGHT_REDUCTION,
	PROP_TEMPORAL_ALPHA,
	PROP_NOISE_GUARD,
};

static GstStaticPadTemplate sink_template = GST_STATIC_PAD_TEMPLATE(
	"sink", GST_PAD_SINK, GST_PAD_ALWAYS,
	GST_STATIC_CAPS("video/x-raw,format=(string)NV12"));

static GstStaticPadTemplate source_template = GST_STATIC_PAD_TEMPLATE(
	"src", GST_PAD_SRC, GST_PAD_ALWAYS,
	GST_STATIC_CAPS("video/x-raw,format=(string)NV12"));

static video_broker_tone_mode_t parse_tone_mode(const gchar *value) {
	if (g_strcmp0(value, "off") == 0) {
		return VIDEO_BROKER_TONE_OFF;
	}
	if (g_strcmp0(value, "global") == 0) {
		return VIDEO_BROKER_TONE_GLOBAL;
	}
	if (g_strcmp0(value, "local") == 0) {
		return VIDEO_BROKER_TONE_LOCAL;
	}
	return VIDEO_BROKER_TONE_AUTO;
}

static gboolean ensure_mapper(GstVideoBrokerTone *self) {
	gint fps;
	video_broker_tone_accelerator_kind_t requested;
	if (self->mapper_ready) {
		return TRUE;
	}
	if (GST_VIDEO_INFO_WIDTH(&self->info) <= 0 ||
	    GST_VIDEO_INFO_HEIGHT(&self->info) <= 0 ||
	    GST_VIDEO_INFO_FORMAT(&self->info) != GST_VIDEO_FORMAT_NV12) {
		return FALSE;
	}
	fps = GST_VIDEO_INFO_FPS_N(&self->info);
	if (GST_VIDEO_INFO_FPS_D(&self->info) > 0) {
		fps /= GST_VIDEO_INFO_FPS_D(&self->info);
	}
	if (fps <= 0) {
		fps = 30;
	}
	video_broker_nv12_tone_mapper_init(
		&self->mapper,
		parse_tone_mode(self->mode),
		self->target_luma,
		self->hysteresis,
		self->grid_columns,
		self->grid_rows,
		self->statistics_hz,
		fps,
		self->max_shadow_gain,
		self->max_highlight_reduction,
		self->temporal_alpha,
		self->noise_guard != FALSE);
	requested = video_broker_tone_accelerator_parse(self->accelerator);
	if (video_broker_tone_accelerator_init(&self->tone_accelerator, requested) != 0) {
		if (video_broker_tone_accelerator_resources_quarantined())
			gst_tone_release_unconfirmed("partial_init");
		GST_ERROR_OBJECT(
			self, "tone accelerator unavailable requested=%s reason=%s",
			video_broker_tone_accelerator_name(requested),
			video_broker_tone_accelerator_error(&self->tone_accelerator));
		video_broker_nv12_tone_mapper_destroy(&self->mapper);
		return FALSE;
	}
	self->mapper_ready = TRUE;
	g_printerr(
		"[video-ingest-gst][tone-accelerator] requested=%s selected=%s fallback=%s\n",
		video_broker_tone_accelerator_name(requested),
		video_broker_tone_accelerator_name(self->tone_accelerator.selected),
		video_broker_tone_accelerator_error(&self->tone_accelerator));
	return TRUE;
}

static gboolean gst_video_broker_tone_set_caps(
	GstBaseTransform *base,
	GstCaps *input_caps,
	GstCaps *output_caps
) {
	GstVideoBrokerTone *self = (GstVideoBrokerTone *)base;
	(void)output_caps;
	if (!gst_video_info_from_caps(&self->info, input_caps) ||
	    GST_VIDEO_INFO_FORMAT(&self->info) != GST_VIDEO_FORMAT_NV12) {
		return FALSE;
	}
	if (self->mapper_ready) {
		if (video_broker_tone_accelerator_destroy(&self->tone_accelerator) != 0)
			gst_tone_release_unconfirmed("caps_change");
		video_broker_nv12_tone_mapper_destroy(&self->mapper);
		self->mapper_ready = FALSE;
	}
	return TRUE;
}

static gboolean copy_luma_to_scratch(
	GstVideoBrokerTone *self,
	const guint8 *source,
	gint stride,
	gint width,
	gint height
) {
	gsize required = (gsize)width * (gsize)height;
	gint row;
	if (required == 0 || required > G_MAXSIZE / 2) {
		return FALSE;
	}
	if (self->scratch_size < required) {
		if (video_broker_tone_accelerator_release_surface(&self->tone_accelerator) != 0)
			gst_tone_release_unconfirmed("scratch_resize");
		guint8 *replacement = g_try_realloc(self->scratch, required);
		if (replacement == NULL) {
			return FALSE;
		}
		self->scratch = replacement;
		self->scratch_size = required;
	}
	for (row = 0; row < height; ++row) {
		memcpy(self->scratch + (gsize)row * (gsize)width,
		       source + (gsize)row * (gsize)stride,
		       (gsize)width);
	}
	return TRUE;
}

static void copy_luma_from_scratch(
	const GstVideoBrokerTone *self,
	guint8 *destination,
	gint stride,
	gint width,
	gint height
) {
	gint row;
	for (row = 0; row < height; ++row) {
		memcpy(destination + (gsize)row * (gsize)stride,
		       self->scratch + (gsize)row * (gsize)width,
		       (gsize)width);
	}
}

static GstFlowReturn gst_video_broker_tone_transform_ip(
	GstBaseTransform *base,
	GstBuffer *buffer
) {
	GstVideoBrokerTone *self = (GstVideoBrokerTone *)base;
	GstVideoFrame frame;
	guint8 *luma;
	guint8 *contiguous;
	gint stride;
	gint width;
	gint height;
	gint64 now_us;
	video_broker_nv12_quality_measure_t raw_quality;
	video_broker_nv12_quality_measure_t output_quality;
	video_broker_tone_accelerator_kind_t accelerator_before;

	if (!ensure_mapper(self)) {
		return GST_FLOW_ERROR;
	}
	if (!gst_video_frame_map(&frame, &self->info, buffer, GST_MAP_READWRITE)) {
		return GST_FLOW_ERROR;
	}
	width = GST_VIDEO_FRAME_WIDTH(&frame);
	height = GST_VIDEO_FRAME_HEIGHT(&frame);
	luma = GST_VIDEO_FRAME_PLANE_DATA(&frame, 0);
	stride = GST_VIDEO_FRAME_PLANE_STRIDE(&frame, 0);
	if (luma == NULL || width <= 0 || height <= 0 || stride < width) {
		gst_video_frame_unmap(&frame);
		return GST_FLOW_ERROR;
	}
	contiguous = luma;
	/* OpenCL USE_HOST_PTR must reference storage owned beyond GstFrame unmap.
	 * Native V4L2/HBN already own their surfaces; only this externally mapped
	 * GStreamer input needs the existing persistent scratch buffer. */
	if (stride != width || self->tone_accelerator.selected == VIDEO_BROKER_TONE_ACCELERATOR_OPENCL) {
		if (!copy_luma_to_scratch(self, luma, stride, width, height)) {
			gst_video_frame_unmap(&frame);
			return GST_FLOW_ERROR;
		}
		contiguous = self->scratch;
	}
	raw_quality = video_broker_nv12_quality_measure(
		contiguous, width, height, self->sample_stride);
	video_broker_nv12_tone_mapper_set_signal_informative(
		&self->mapper, video_broker_nv12_quality_is_informative(&raw_quality));
	(void)video_broker_nv12_tone_mapper_update(
		&self->mapper, contiguous, width, height, self->sample_stride);
	accelerator_before = self->tone_accelerator.selected;
	if (video_broker_tone_accelerator_apply(
			&self->tone_accelerator, &self->mapper, contiguous, width, height) < 0) {
		/* A failed enqueue may still reference mapped host storage. */
		if (video_broker_tone_accelerator_destroy(&self->tone_accelerator) != 0)
			gst_tone_release_unconfirmed("apply_failure");
		gst_video_frame_unmap(&frame);
		return GST_FLOW_ERROR;
	}
	if (contiguous == self->scratch) {
		copy_luma_from_scratch(self, luma, stride, width, height);
	}
	now_us = g_get_monotonic_time();
	if (accelerator_before != self->tone_accelerator.selected ||
	    self->last_telemetry_us == 0 || now_us - self->last_telemetry_us >= 5000000) {
		/* Post-tone observation serves this log only, not the per-frame guard. */
		output_quality = video_broker_nv12_quality_measure(
			contiguous, width, height, self->sample_stride);
		self->last_telemetry_us = now_us;
		g_printerr(
			"[video-ingest-gst][tone-mapper] event=tone_mapping_summary requested_mode=%s active_mode=%s accelerator_requested=%s accelerator_selected=%s raw_luma=%.1Lf raw_content_luma=%.1Lf output_luma=%.1Lf output_content_luma=%.1Lf raw_black_ratio=%.5Lf output_black_ratio=%.5Lf raw_white_ratio=%.5Lf output_white_ratio=%.5Lf signal_informative=%s target=%d gain_min=%.3f gain_max=%.3f statistics_hz=%d effective_statistics_hz=%d statistics_updates=%llu active=%s\n",
			video_broker_tone_mode_name(self->mapper.requested_mode),
			video_broker_tone_mode_name(self->mapper.active_mode),
			video_broker_tone_accelerator_name(self->tone_accelerator.requested),
			video_broker_tone_accelerator_name(self->tone_accelerator.selected),
			raw_quality.luma, raw_quality.content_luma,
			output_quality.luma, output_quality.content_luma,
			raw_quality.black_ratio, output_quality.black_ratio,
			raw_quality.white_ratio, output_quality.white_ratio,
			self->mapper.signal_informative ? "true" : "false",
			self->mapper.target_luma,
			self->mapper.minimum_gain, self->mapper.maximum_gain,
			self->mapper.statistics_hz, self->mapper.effective_statistics_hz,
			(unsigned long long)self->mapper.statistics_updates,
			video_broker_nv12_tone_mapper_active(&self->mapper) ? "true" : "false");
	}
	gst_video_frame_unmap(&frame);
	return GST_FLOW_OK;
}

static void gst_video_broker_tone_set_property(
	GObject *object,
	guint property_id,
	const GValue *value,
	GParamSpec *spec
) {
	GstVideoBrokerTone *self = (GstVideoBrokerTone *)object;
	switch (property_id) {
	case PROP_MODE:
		g_free(self->mode);
		self->mode = g_value_dup_string(value);
		break;
	case PROP_ACCELERATOR:
		g_free(self->accelerator);
		self->accelerator = g_value_dup_string(value);
		break;
	case PROP_TARGET_LUMA: self->target_luma = g_value_get_int(value); break;
	case PROP_HYSTERESIS: self->hysteresis = g_value_get_int(value); break;
	case PROP_GRID_COLUMNS: self->grid_columns = g_value_get_int(value); break;
	case PROP_GRID_ROWS: self->grid_rows = g_value_get_int(value); break;
	case PROP_STATISTICS_HZ: self->statistics_hz = g_value_get_int(value); break;
	case PROP_SAMPLE_STRIDE: self->sample_stride = g_value_get_int(value); break;
	case PROP_MAX_SHADOW_GAIN: self->max_shadow_gain = g_value_get_double(value); break;
	case PROP_MAX_HIGHLIGHT_REDUCTION: self->max_highlight_reduction = g_value_get_double(value); break;
	case PROP_TEMPORAL_ALPHA: self->temporal_alpha = g_value_get_double(value); break;
	case PROP_NOISE_GUARD: self->noise_guard = g_value_get_boolean(value); break;
	default: G_OBJECT_WARN_INVALID_PROPERTY_ID(object, property_id, spec); break;
	}
}

static void gst_video_broker_tone_get_property(
	GObject *object,
	guint property_id,
	GValue *value,
	GParamSpec *spec
) {
	GstVideoBrokerTone *self = (GstVideoBrokerTone *)object;
	switch (property_id) {
	case PROP_MODE: g_value_set_string(value, self->mode); break;
	case PROP_ACCELERATOR: g_value_set_string(value, self->accelerator); break;
	case PROP_TARGET_LUMA: g_value_set_int(value, self->target_luma); break;
	case PROP_HYSTERESIS: g_value_set_int(value, self->hysteresis); break;
	case PROP_GRID_COLUMNS: g_value_set_int(value, self->grid_columns); break;
	case PROP_GRID_ROWS: g_value_set_int(value, self->grid_rows); break;
	case PROP_STATISTICS_HZ: g_value_set_int(value, self->statistics_hz); break;
	case PROP_SAMPLE_STRIDE: g_value_set_int(value, self->sample_stride); break;
	case PROP_MAX_SHADOW_GAIN: g_value_set_double(value, self->max_shadow_gain); break;
	case PROP_MAX_HIGHLIGHT_REDUCTION: g_value_set_double(value, self->max_highlight_reduction); break;
	case PROP_TEMPORAL_ALPHA: g_value_set_double(value, self->temporal_alpha); break;
	case PROP_NOISE_GUARD: g_value_set_boolean(value, self->noise_guard); break;
	default: G_OBJECT_WARN_INVALID_PROPERTY_ID(object, property_id, spec); break;
	}
}

static void gst_video_broker_tone_finalize(GObject *object) {
	GstVideoBrokerTone *self = (GstVideoBrokerTone *)object;
	if (self->mapper_ready) {
		if (video_broker_tone_accelerator_destroy(&self->tone_accelerator) != 0)
			gst_tone_release_unconfirmed("finalize");
		video_broker_nv12_tone_mapper_destroy(&self->mapper);
	}
	g_clear_pointer(&self->mode, g_free);
	g_clear_pointer(&self->accelerator, g_free);
	g_clear_pointer(&self->scratch, g_free);
	G_OBJECT_CLASS(gst_video_broker_tone_parent_class)->finalize(object);
}

static void gst_video_broker_tone_class_init(GstVideoBrokerToneClass *klass) {
	GObjectClass *object_class = G_OBJECT_CLASS(klass);
	GstElementClass *element_class = GST_ELEMENT_CLASS(klass);
	GstBaseTransformClass *transform_class = GST_BASE_TRANSFORM_CLASS(klass);
	object_class->set_property = gst_video_broker_tone_set_property;
	object_class->get_property = gst_video_broker_tone_get_property;
	object_class->finalize = gst_video_broker_tone_finalize;
	g_object_class_install_property(object_class, PROP_MODE,
		g_param_spec_string("mode", "Mode", "Tone mapping mode", "auto", G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_ACCELERATOR,
		g_param_spec_string("accelerator", "Accelerator", "auto, cpu or opencl", "auto", G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_TARGET_LUMA,
		g_param_spec_int("target-luma", "Target luma", "Target content luma", 16, 235, 128, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_HYSTERESIS,
		g_param_spec_int("hysteresis", "Hysteresis", "Luma hysteresis", 1, 64, 12, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_GRID_COLUMNS,
		g_param_spec_int("grid-columns", "Grid columns", "Spatial grid columns", 1, 32, 16, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_GRID_ROWS,
		g_param_spec_int("grid-rows", "Grid rows", "Spatial grid rows", 1, 24, 9, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_STATISTICS_HZ,
		g_param_spec_int("statistics-hz", "Statistics Hz", "Maximum statistics updates per second", 1, 30, 8, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_SAMPLE_STRIDE,
		g_param_spec_int("sample-stride", "Sample stride", "Quality and histogram sample stride", 1, 64, 8, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_MAX_SHADOW_GAIN,
		g_param_spec_double("max-shadow-gain", "Max shadow gain", "Maximum shadow gain", 1.0, 3.0, 2.4, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_MAX_HIGHLIGHT_REDUCTION,
		g_param_spec_double("max-highlight-reduction", "Max highlight reduction", "Maximum highlight reduction", 0.0, 0.75, 0.2, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_TEMPORAL_ALPHA,
		g_param_spec_double("temporal-alpha", "Temporal alpha", "Temporal adaptation coefficient", 0.01, 1.0, 0.35, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	g_object_class_install_property(object_class, PROP_NOISE_GUARD,
		g_param_spec_boolean("noise-guard", "Noise guard", "Avoid amplifying non-informative noise", TRUE, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
	gst_element_class_set_static_metadata(
		element_class,
		"Video Broker spatial tone mapper", "Filter/Effect/Video",
		"Camera-independent NV12 zonal tone mapping", "Half Pocket");
	gst_element_class_add_static_pad_template(element_class, &sink_template);
	gst_element_class_add_static_pad_template(element_class, &source_template);
	transform_class->set_caps = GST_DEBUG_FUNCPTR(gst_video_broker_tone_set_caps);
	transform_class->transform_ip = GST_DEBUG_FUNCPTR(gst_video_broker_tone_transform_ip);
}

static void gst_video_broker_tone_init(GstVideoBrokerTone *self) {
	self->mode = g_strdup("auto");
	self->accelerator = g_strdup("auto");
	self->target_luma = 128;
	self->hysteresis = 12;
	self->grid_columns = 16;
	self->grid_rows = 9;
	self->statistics_hz = 8;
	self->sample_stride = 8;
	self->max_shadow_gain = 2.4;
	self->max_highlight_reduction = 0.2;
	self->temporal_alpha = 0.35;
	self->noise_guard = TRUE;
	gst_base_transform_set_in_place(GST_BASE_TRANSFORM(self), TRUE);
	gst_base_transform_set_passthrough(GST_BASE_TRANSFORM(self), FALSE);
}

static gboolean plugin_init(GstPlugin *plugin) {
	return gst_element_register(plugin, "videobrokertone", GST_RANK_PRIMARY, gst_video_broker_tone_get_type());
}

GST_PLUGIN_DEFINE(
	GST_VERSION_MAJOR,
	GST_VERSION_MINOR,
	videobrokertone,
	"Video Broker NV12 spatial tone mapper",
	plugin_init,
	"1.0.0",
	"Proprietary",
	"video-broker",
	"https://halfpocket.net")
