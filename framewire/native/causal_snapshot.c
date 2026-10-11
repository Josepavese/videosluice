#include "causal_snapshot.h"
#include <string.h>

static uint64_t read64(const uint8_t *b) {
    uint64_t n=0;
    for(size_t i=0;i<8;i++) n=(n<<8)|b[i];
    return n;
}
static void write64(uint8_t *b,uint64_t n) {
    for(size_t i=0;i<8;i++) b[i]=(uint8_t)(n>>((7-i)*8));
}
static int allocation(const uint8_t ns[16],uint64_t generation,uint64_t capacity) {
    uint8_t sum=0;
    for(size_t i=0;i<16;i++) sum|=ns[i];
    return sum&&generation&&capacity>=VBCF_HEADER_SIZE&&capacity<=VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD;
}
static int validate(const vbsc_control *c) {
    if(!c||!allocation(c->namespace_id,c->generation,c->capacity)||c->used<VBCF_HEADER_SIZE||c->used>c->capacity||!c->begin||!c->end) return VBSC_INVALID;
    return (c->begin&1)||c->begin!=c->end?VBSC_BUSY:VBSC_OK;
}
int vbsc_parse(const uint8_t *b,size_t n,vbsc_control *out) {
    if(!out) return VBSC_INVALID;
    memset(out,0,sizeof(*out));
    if(!b||n!=VBSC_SIZE||memcmp(b,"VBSC",4)||b[4]||b[5]!=1||b[6]||b[7]!=64) return VBSC_INVALID;
    vbsc_control c={0};
    c.begin=read64(b+8);c.capacity=read64(b+16);c.used=read64(b+24);c.generation=read64(b+32);
    memcpy(c.namespace_id,b+40,16);c.end=read64(b+56);
    int result=validate(&c);
    if(result) return result;
    *out=c;return VBSC_OK;
}
int vbsc_marshal(const vbsc_control *c,uint8_t b[VBSC_SIZE]) {
    if(!b) return VBSC_INVALID;
    memset(b,0,VBSC_SIZE);
    int result=validate(c);
    if(result) return result;
    memcpy(b,"VBSC",4);b[5]=1;b[7]=64;
    write64(b+8,c->begin);write64(b+16,c->capacity);write64(b+24,c->used);write64(b+32,c->generation);
    memcpy(b+40,c->namespace_id,16);write64(b+56,c->end);
    return VBSC_OK;
}
static int same_mapping(const vbsc_control *c,const vbsc_mapping *m) {
    return c->generation==m->generation&&c->capacity==m->capacity&&!memcmp(c->namespace_id,m->namespace_id,16);
}
int vbsc_validate_staging(const uint8_t *before,size_t bn,const uint8_t *packet,size_t pn,
    const uint8_t *after,size_t an,const vbsc_mapping *m,
    vbcf_header *out,const uint8_t **payload) {
    if(out) memset(out,0,sizeof(*out));
    if(payload) *payload=NULL;
    if(!out||!payload) return VBSC_INVALID;
    if(!m||!allocation(m->namespace_id,m->generation,m->capacity)) return VBSC_MAPPING;
    vbsc_control a,b;
    int result=vbsc_parse(before,bn,&a);
    if(result) return result;
    result=vbsc_parse(after,an,&b);
    if(result) return result;
    if(!same_mapping(&a,m)||!same_mapping(&b,m)) return VBSC_MAPPING;
    if(memcmp(before,after,VBSC_SIZE)) return VBSC_BUSY;
    if(!packet||pn!=a.used) return VBSC_INVALID;
    vbcf_header h;
    if(pn<VBCF_HEADER_SIZE||vbcf_parse_header(packet,VBCF_HEADER_SIZE,&h)||h.payload_bytes!=pn-VBCF_HEADER_SIZE) return VBSC_INVALID;
    if(memcmp(h.namespace_id,m->namespace_id,16)) return VBSC_MAPPING;
    *out=h;*payload=packet+VBCF_HEADER_SIZE;return VBSC_OK;
}
int vbsc_validate_snapshot(const uint8_t *before,size_t bn,const uint8_t *packet,size_t pn,
    const uint8_t *after,size_t an,const vbsc_mapping *m,vbcf_sha256_fn hash,void *context,
    vbcf_header *out,const uint8_t **payload) {
    int result=vbsc_validate_staging(before,bn,packet,pn,after,an,m,out,payload);
    if(result)return result;
    if(vbcf_validate_payload(out,*payload,pn-VBCF_HEADER_SIZE,hash,context)) {
        memset(out,0,sizeof(*out));*payload=NULL;return VBSC_INVALID;
    }
    return VBSC_OK;
}
