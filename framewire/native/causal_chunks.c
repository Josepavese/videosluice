#include "causal_chunks.h"
#include <string.h>

static int nonzero(const uint8_t *p,size_t n) {
    uint8_t bits=0;for(size_t i=0;i<n;i++)bits|=p[i];return bits!=0;
}
int fapd_encode(uint8_t *out,size_t capacity,const uint8_t manifest[32],
    uint32_t offset,const uint8_t *payload,size_t size,size_t *written) {
    if(written)*written=0;
    if(!out||!manifest||!payload||!written||!nonzero(manifest,32)||!size||
       size>FAPD_MAX_PACKET-FAPD_HEADER_SIZE||offset>FAPD_MAX_EXTENT||
       size>FAPD_MAX_EXTENT-offset||capacity<FAPD_HEADER_SIZE+size)return -1;
    size_t n=FAPD_HEADER_SIZE+size;
    memcpy(out,"FAPD",4);out[4]=1;out[5]=0;out[6]=(uint8_t)(n>>8);out[7]=(uint8_t)n;
    memcpy(out+8,manifest,32);
    out[40]=(uint8_t)(offset>>24);out[41]=(uint8_t)(offset>>16);
    out[42]=(uint8_t)(offset>>8);out[43]=(uint8_t)offset;
    memcpy(out+FAPD_HEADER_SIZE,payload,size);*written=n;return 0;
}
int vbcf_send_source_packet(const uint8_t header[VBCF_HEADER_SIZE],const uint8_t *pixels,size_t size,
    uint8_t *workspace,size_t workspace_size,vbcf_sha256_fn hash,void *hash_context,
    vbcf_send_record_fn send,void *send_context,vbcf_send_progress *progress) {
    if(!progress)return -1;
    memset(progress,0,sizeof(*progress));progress->stage=VBCF_SEND_CONTRACT;
    if(!header||!pixels||!workspace||workspace_size<FAPD_MAX_PACKET||!hash||!send)return -1;
    vbcf_header parsed;
    if(vbcf_parse_header(header,VBCF_HEADER_SIZE,&parsed)||(parsed.kind!=1&&parsed.kind!=5)||parsed.payload_bytes!=size)return -1;
    progress->stage=VBCF_SEND_MANIFEST_HASH;
    uint8_t manifest[32];
    if(hash(hash_context,header,VBCF_HEADER_SIZE,manifest)||!nonzero(manifest,sizeof(manifest)))return -1;
    progress->stage=VBCF_SEND_HEADER;progress->attempted=1;
    int result=send(send_context,header,VBCF_HEADER_SIZE);
    if(result)return result;
    progress->records_sent=1;
    while(progress->bytes_sent<size) {
        size_t n=size-progress->bytes_sent;
        if(n>FAPD_MAX_PACKET-FAPD_HEADER_SIZE)n=FAPD_MAX_PACKET-FAPD_HEADER_SIZE;
        size_t written;
        progress->stage=VBCF_SEND_CHUNK;
        if(fapd_encode(workspace,workspace_size,manifest,progress->bytes_sent,pixels+progress->bytes_sent,n,&written))return -1;
        progress->stage=VBCF_SEND_DATA;progress->attempted_offset=progress->bytes_sent;
        result=send(send_context,workspace,written);
        if(result)return result;
        progress->bytes_sent+=(uint32_t)n;progress->records_sent++;
    }
    progress->stage=VBCF_SEND_COMPLETE;
    return 0;
}
