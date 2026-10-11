#ifndef VIDEO_BROKER_NV12_CAUSAL_TRANSFORM_H
#define VIDEO_BROKER_NV12_CAUSAL_TRANSFORM_H
#include "nv12_orientation.h"
#include "causal_envelope.h"

/* Canonical frameaccess.Transform digest of the SAME layout used by the native
 * resize/orientation implementation. This describes one spatial transform;
 * callers must not use it to label a different, chained or unknown transform.
 * Colour/tone operations do not change the source-space geometry. */
static inline int video_broker_nv12_transform_digest(
    int src_w,int src_h,int src_stride,int dst_w,int dst_h,
    video_broker_video_orientation_t orientation,
    video_broker_nv12_aspect_policy_t policy,
    vbcf_sha256_fn hash,void *hash_context,uint8_t digest[32]) {
    if(!digest)return -1;
    memset(digest,0,32);
    if(!hash||src_w<2||src_h<2||src_w>8192||src_h>8192||src_stride<src_w||
       dst_w<2||dst_h<2||dst_w>8192||dst_h>8192||
       ((src_w|src_h|src_stride|dst_w|dst_h)&1)||
       orientation<VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL||orientation>VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL||
       policy<VIDEO_BROKER_NV12_ASPECT_EXACT||policy>VIDEO_BROKER_NV12_ASPECT_LETTERBOX)return -1;
    video_broker_nv12_aspect_layout_t l=video_broker_nv12_aspect_layout(src_w,src_h,dst_w,dst_h,policy);
    uint64_t rotation=(orientation==VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180||orientation==VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL)?180:0;
    uint64_t mirror=(orientation==VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL||orientation==VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL)?1:0;
    const uint64_t fields[]={src_w,src_h,src_stride,l.src_x,l.src_y,l.src_x+l.src_w,l.src_y+l.src_h,
        rotation,mirror,l.dst_w,l.dst_h,dst_w,dst_h,l.dst_x,l.dst_y};
    static const char tag[]="video-broker.frame-transform.v1";
    uint8_t encoded[8+sizeof(tag)-1+sizeof(fields)];
    size_t offset=0;
    uint64_t n=sizeof(tag)-1;
    for(int shift=56;shift>=0;shift-=8)encoded[offset++]=(uint8_t)(n>>shift);
    memcpy(encoded+offset,tag,sizeof(tag)-1);offset+=sizeof(tag)-1;
    for(size_t j=0;j<sizeof(fields)/sizeof(fields[0]);j++)
        for(int shift=56;shift>=0;shift-=8)encoded[offset++]=(uint8_t)(fields[j]>>shift);
    if(hash(hash_context,encoded,offset,digest)){memset(digest,0,32);return -1;}
    return 0;
}
#endif
