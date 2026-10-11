#include "causal_envelope.h"
#include <limits.h>
#include <string.h>

static const uint8_t empty_sha[32]={
 0xe3,0xb0,0xc4,0x42,0x98,0xfc,0x1c,0x14,0x9a,0xfb,0xf4,0xc8,0x99,0x6f,0xb9,0x24,
 0x27,0xae,0x41,0xe4,0x64,0x9b,0x93,0x4c,0xa4,0x95,0x99,0x1b,0x78,0x52,0xb8,0x55};
static int nonzero(const uint8_t *p,size_t n){uint8_t v=0;for(size_t i=0;i<n;i++)v|=p[i];return v!=0;}
static int token(const char p[65],int empty){
 size_t n=0;while(n<65&&p[n]){unsigned char c=(unsigned char)p[n];
  if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='.'||c=='_'||c=='-'))return 0;
  n++;}
 return n<=64&&(n||empty);
}
int vbcf_validate(const vbcf_header *h){
 if(!h||(h->kind==5)!=(h->output_sequence!=0))return -1;
 if(!h||!token(h->source,0)||!token(h->process,0)||!token(h->stream,1)||!nonzero(h->namespace_id,16)||!nonzero(h->graph,32)||!h->source_epoch||h->payload_bytes>VBCF_MAX_PAYLOAD||(h->flags&~UINT32_C(31))||h->correlation>1)return -1;
 if(h->kind==3){
  if(h->codec||h->surface||h->correlation||h->flags||h->payload_bytes||h->width||h->height||h->stride_y||h->stride_uv||h->origin_sequence||!h->publication_epoch||h->delivery_id||h->capture_ns||h->media_pts||nonzero(h->transform,32)||!token(h->stream,0)||memcmp(h->payload_sha,empty_sha,32))return -1;
  return 0;
 }
 if(h->kind==4){
  uint64_t expected=(uint64_t)h->stride_y*h->height+(uint64_t)h->stride_uv*(h->height/2);
  if(h->codec!=3||(h->surface!=1&&h->surface!=2)||h->correlation||h->flags||h->payload_bytes||h->origin_sequence||h->publication_epoch||h->delivery_id||h->capture_ns||h->media_pts||h->stream[0]||memcmp(h->payload_sha,empty_sha,32)||!nonzero(h->transform,32)||!h->width||!h->height||h->width>16384||h->height>16384||((h->width|h->height|h->stride_y|h->stride_uv)&1)||h->stride_y<h->width||h->stride_uv<h->width||expected>VBCF_MAX_PAYLOAD)return -1;
  return 0;
 }
 if((h->kind!=1&&h->kind!=2&&h->kind!=5)||!h->width||!h->height||h->width>16384||h->height>16384||!h->payload_bytes||!nonzero(h->transform,32)||(h->correlation==1)!=(h->origin_sequence!=0))return -1;
 if((!(h->flags&1)&&h->capture_ns)||((h->flags&1)&&(!h->capture_ns||!h->correlation))||(!(h->flags&2)&&h->media_pts))return -1;
 if(h->kind==1){if(h->stream[0]||h->publication_epoch||h->delivery_id||h->correlation!=1||h->codec!=3||(h->surface!=1&&h->surface!=2))return -1;}
 else if(h->kind==5){if(!token(h->stream,0)||h->publication_epoch||h->delivery_id||h->surface!=3)return -1;}
 else if(!token(h->stream,0)||!h->publication_epoch||!h->delivery_id||h->surface!=3)return -1;
 if(h->codec==3){
  if(((h->width|h->height|h->stride_y|h->stride_uv)&1)||h->stride_y<h->width||h->stride_uv<h->width||(h->flags&28))return -1;
  uint64_t expected=(uint64_t)h->stride_y*h->height+(uint64_t)h->stride_uv*(h->height/2);
  if(expected!=h->payload_bytes)return -1;
 }else if((h->codec!=1&&h->codec!=2)||h->stride_y||h->stride_uv)return -1;
 return 0;
}
static uint32_t get32(const uint8_t *b){return (uint32_t)b[0]<<24|(uint32_t)b[1]<<16|(uint32_t)b[2]<<8|b[3];}
static uint64_t get64(const uint8_t *b){return (uint64_t)get32(b)<<32|get32(b+4);}
static void put32(uint8_t *b,uint32_t v){b[0]=(uint8_t)(v>>24);b[1]=(uint8_t)(v>>16);b[2]=(uint8_t)(v>>8);b[3]=(uint8_t)v;}
static void put64(uint8_t *b,uint64_t v){put32(b,(uint32_t)(v>>32));put32(b+4,(uint32_t)v);}
static int read_token(char out[65],const uint8_t *b){
 size_t n=0;while(n<64&&b[n])n++;for(size_t i=n;i<64;i++)if(b[i])return -1;
 memcpy(out,b,n);out[n]=0;return token(out,1)?0:-1;
}
int vbcf_parse_header(const uint8_t *b,size_t size,vbcf_header *out){
 if(!out)return -1;
 memset(out,0,sizeof(*out));
 if(!b||size!=VBCF_HEADER_SIZE||memcmp(b,"VBCF",4)||b[4]||b[5]!=1||b[6]!=1||b[7]!=192||nonzero(b+36,4)||nonzero(b+400,48))return -1;
 vbcf_header h={0};h.kind=b[8];h.codec=b[9];h.surface=b[10];h.correlation=b[11];
 h.flags=get32(b+12);h.payload_bytes=get32(b+16);h.width=get32(b+20);h.height=get32(b+24);h.stride_y=get32(b+28);h.stride_uv=get32(b+32);
 h.source_epoch=get64(b+40);h.origin_sequence=get64(b+48);h.publication_epoch=get64(b+56);h.delivery_id=get64(b+64);h.capture_ns=get64(b+72);
 h.output_sequence=get64(b+392);
 uint64_t pts=get64(b+80);h.media_pts=pts<=INT64_MAX?(int64_t)pts:-1-(int64_t)(UINT64_MAX-pts);
 memcpy(h.namespace_id,b+88,16);memcpy(h.graph,b+104,32);memcpy(h.transform,b+136,32);memcpy(h.payload_sha,b+168,32);
 if(read_token(h.source,b+200)||read_token(h.stream,b+264)||read_token(h.process,b+328)||vbcf_validate(&h))return -1;
 *out=h;return 0;
}
int vbcf_marshal_header(const vbcf_header *h,uint8_t b[VBCF_HEADER_SIZE]){
 if(!b)return -1;
 memset(b,0,VBCF_HEADER_SIZE);if(vbcf_validate(h))return -1;
 memcpy(b,"VBCF",4);b[5]=1;b[6]=1;b[7]=192;b[8]=h->kind;b[9]=h->codec;b[10]=h->surface;b[11]=h->correlation;
 put32(b+12,h->flags);put32(b+16,h->payload_bytes);put32(b+20,h->width);put32(b+24,h->height);put32(b+28,h->stride_y);put32(b+32,h->stride_uv);
 put64(b+40,h->source_epoch);put64(b+48,h->origin_sequence);put64(b+56,h->publication_epoch);put64(b+64,h->delivery_id);put64(b+72,h->capture_ns);put64(b+80,(uint64_t)h->media_pts);
 memcpy(b+88,h->namespace_id,16);memcpy(b+104,h->graph,32);memcpy(b+136,h->transform,32);memcpy(b+168,h->payload_sha,32);
 memcpy(b+200,h->source,strlen(h->source));memcpy(b+264,h->stream,strlen(h->stream));memcpy(b+328,h->process,strlen(h->process));put64(b+392,h->output_sequence);return 0;
}
int vbcf_validate_payload(const vbcf_header *h,const uint8_t *p,size_t n,vbcf_sha256_fn hash,void *ctx){
 if(vbcf_validate(h)||n!=h->payload_bytes||(!p&&n)||!hash)return -1;
 uint8_t digest[32]={0};if(hash(ctx,p,n,digest))return -1;
 return memcmp(digest,h->payload_sha,32)?-1:0;
}

int vbcf_marshal_surface_header(const vbcf_header *layout,
 uint64_t origin_epoch,uint64_t origin_sequence,
 const uint8_t *pixels,size_t size,vbcf_sha256_fn hash,void *ctx,
 uint8_t bytes[VBCF_HEADER_SIZE]){
 if(!bytes)return VBCF_SURFACE_INVALID;
 memset(bytes,0,VBCF_HEADER_SIZE);
 if(!layout||!pixels||!hash||!size||size>VBCF_MAX_PAYLOAD||
    layout->kind!=4||vbcf_validate(layout))return VBCF_SURFACE_INVALID;
 if(!origin_epoch||!origin_sequence)return VBCF_SURFACE_ORIGIN_UNAVAILABLE;
 if(origin_epoch!=layout->source_epoch)return VBCF_SURFACE_EPOCH_MISMATCH;
 vbcf_header h=*layout;
 h.kind=1;
 h.correlation=1;
 h.origin_sequence=origin_sequence;
 h.payload_bytes=(uint32_t)size;
 /* Validate geometry and declared length BEFORE reading the borrowed pixels. */
 if(vbcf_validate(&h))return VBCF_SURFACE_INVALID;
 if(hash(ctx,pixels,size,h.payload_sha))return VBCF_SURFACE_HASH_FAILED;
 return vbcf_marshal_header(&h,bytes);
}
