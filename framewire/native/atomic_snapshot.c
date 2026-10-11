#include "atomic_snapshot.h"
#include <string.h>

_Static_assert(sizeof(_Atomic uint64_t)==8,"atomic64 ABI unavailable");
static uint64_t raw(const uint8_t b[8]) { uint64_t v;memcpy(&v,b,8);return v; }
static uint64_t counter(uint64_t raw_value) {
    uint8_t b[8];uint64_t n=0;memcpy(b,&raw_value,8);
    for(size_t i=0;i<8;i++) n=(n<<8)|b[i];
    return n;
}
static uint64_t encoded_counter(uint64_t n) {
    uint8_t b[8];for(size_t i=0;i<8;i++)b[i]=(uint8_t)(n>>((7-i)*8));
    return raw(b);
}
static void load_words(const vbsc_atomic_region *r,size_t offset,uint8_t *out,size_t count) {
    /* The admitted mapping is stable for the operation. Resolve its base
     * once; preserve every shared 64-bit seq_cst access and its address order. */
    const _Atomic uint64_t *words=r->words+offset/8;
    size_t full=count-count%8;
    for(size_t i=0;i<full;i+=8){
        uint64_t word=atomic_load_explicit(words+i/8,memory_order_seq_cst);
        memcpy(out+i,&word,sizeof(word));
    }
    if(full<count){
        uint64_t word=atomic_load_explicit(words+full/8,memory_order_seq_cst);
        memcpy(out+full,&word,count-full);
    }
}
static void store_words(vbsc_atomic_region *r,size_t offset,const uint8_t *p,size_t count) {
    _Atomic uint64_t *words=r->words+offset/8;
    /* Keep the exact atomic width and total order. Constant-size private
     * copies compile to unaligned-safe word loads, rather than a checked
     * variable-size memcpy call for every eight bytes of a native frame.
     * Only the final partial word is padded; never read beyond private input. */
    size_t full=count-count%8;
    for(size_t i=0;i<full;i+=8){
        uint64_t word;
        memcpy(&word,p+i,sizeof(word));
        atomic_store_explicit(words+i/8,word,memory_order_seq_cst);
    }
    if(full<count){
        uint64_t word=0;
        memcpy(&word,p+full,count-full);
        atomic_store_explicit(words+full/8,word,memory_order_seq_cst);
    }
}
static int admitted(const vbsc_atomic_region *r,const vbsc_mapping *m) {
    if(!r||!r->words||!m||m->capacity<VBCF_HEADER_SIZE||m->capacity>VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD||!m->generation)return 0;
    uint8_t ns=0;for(size_t i=0;i<16;i++)ns|=m->namespace_id[i];
    return ns&&r->bytes==VBSC_SIZE+((m->capacity+7)/8)*8;
}
int vbsc_atomic_bind(void *memory,size_t bytes,vbsc_atomic_region *out) {
    if(!out)return VBSC_INVALID;
    memset(out,0,sizeof(*out));
    if(!memory||(uintptr_t)memory%_Alignof(_Atomic uint64_t)||bytes%8||bytes<VBSC_SIZE+VBCF_HEADER_SIZE||bytes>VBSC_SIZE+VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD)return VBSC_INVALID;
    _Atomic uint64_t probe=0;
    if(!atomic_is_lock_free(&probe))return VBSC_INVALID;
    out->words=memory;out->bytes=bytes;return VBSC_OK;
}
int vbsc_atomic_initialize(vbsc_atomic_region *r) {
    if(!r||!r->words||r->bytes%8)return VBSC_INVALID;
    for(size_t i=0;i<r->bytes/8;i++)atomic_init(r->words+i,0);
    return VBSC_OK;
}
static int publish_parts(vbsc_atomic_region *r,const vbsc_mapping *m,const uint8_t *header,const uint8_t *pixels,size_t bytes,vbsc_commit_guard_fn guard,void *ctx) {
    size_t n=VBCF_HEADER_SIZE+bytes;
    uint64_t seq=counter(atomic_load_explicit(r->words+1,memory_order_seq_cst));
    if(seq&1)return VBSC_BUSY;
    if(seq>UINT64_MAX-2)return VBSC_MAPPING;
    uint8_t previous[VBSC_SIZE];load_words(r,0,previous,VBSC_SIZE);
    if(seq){
        vbsc_control old;
        if(vbsc_parse(previous,VBSC_SIZE,&old)||old.begin!=seq)return VBSC_BUSY;
        if(old.capacity!=m->capacity||old.generation!=m->generation||memcmp(old.namespace_id,m->namespace_id,16))return VBSC_MAPPING;
    }else{
        for(size_t i=0;i<VBSC_SIZE;i++)if(previous[i])return VBSC_INVALID;
    }
    /* Sequentially consistent accesses throughout form one total order. No
     * successful before/after equal even counter can enclose a writer; no
     * sequence reuse and no non-atomic concurrent payload access are allowed.
     * Optimize ordering only after paired correctness/performance evidence. */
    vbsc_control c={0};c.begin=seq+2;c.end=c.begin;c.capacity=m->capacity;c.used=n;c.generation=m->generation;memcpy(c.namespace_id,m->namespace_id,16);
    uint8_t b[VBSC_SIZE];if(vbsc_marshal(&c,b))return VBSC_INVALID;
    if(guard&&guard(ctx))return VBSC_INVALID;
    atomic_store_explicit(r->words+1,encoded_counter(seq+1),memory_order_seq_cst);
    for(size_t i=0;i<8;i++)if(i!=1&&i!=7)atomic_store_explicit(r->words+i,raw(b+i*8),memory_order_seq_cst);
    atomic_store_explicit(r->words+7,encoded_counter(seq+1),memory_order_seq_cst);
    store_words(r,VBSC_SIZE,header,VBCF_HEADER_SIZE);
    store_words(r,VBSC_SIZE+VBCF_HEADER_SIZE,pixels,bytes);
    if(guard&&guard(ctx))return VBSC_INVALID;
    atomic_store_explicit(r->words+7,encoded_counter(seq+2),memory_order_seq_cst);
    atomic_store_explicit(r->words+1,encoded_counter(seq+2),memory_order_seq_cst);
    return VBSC_OK;
}
int vbsc_atomic_publish(vbsc_atomic_region *r,const vbsc_mapping *m,const uint8_t *p,size_t n,vbcf_sha256_fn hash,void *ctx) {
    if(!admitted(r,m)||!p||n<VBCF_HEADER_SIZE||n>m->capacity)return VBSC_INVALID;
    vbcf_header h;
    if(vbcf_parse_header(p,VBCF_HEADER_SIZE,&h)||vbcf_validate_payload(&h,p+VBCF_HEADER_SIZE,n-VBCF_HEADER_SIZE,hash,ctx)||memcmp(h.namespace_id,m->namespace_id,16))return VBSC_INVALID;
    return publish_parts(r,m,p,p+VBCF_HEADER_SIZE,n-VBCF_HEADER_SIZE,NULL,NULL);
}
int vbsc_atomic_produce(vbsc_atomic_region *r,const vbsc_mapping *m,const vbcf_header *metadata,const uint8_t *pixels,size_t n,vbcf_sha256_fn hash,void *ctx) {
    return vbsc_atomic_produce_guarded(r,m,metadata,pixels,n,hash,ctx,NULL);
}
int vbsc_atomic_produce_guarded(vbsc_atomic_region *r,const vbsc_mapping *m,const vbcf_header *metadata,const uint8_t *pixels,size_t n,vbcf_sha256_fn hash,void *ctx,vbsc_commit_guard_fn guard) {
    if(!admitted(r,m)||!metadata||!hash||(!pixels&&n)||n>m->capacity-VBCF_HEADER_SIZE||
       metadata->payload_bytes!=n||memcmp(metadata->namespace_id,m->namespace_id,16))return VBSC_INVALID;
    vbcf_header h=*metadata;
    uint8_t header[VBCF_HEADER_SIZE];
    if(hash(ctx,pixels,n,h.payload_sha)||vbcf_marshal_header(&h,header))return VBSC_INVALID;
    return publish_parts(r,m,header,pixels,n,guard,ctx);
}
int vbsc_atomic_stage(const vbsc_atomic_region *r,const vbsc_mapping *m,uint8_t *p,size_t cap,size_t *used,vbcf_header *out,uint64_t after_serial,uint64_t *serial) {
    if(used)*used=0;
    if(serial)*serial=0;
    if(out)memset(out,0,sizeof(*out));
    if(!used||!out||!p||!admitted(r,m))return VBSC_INVALID;
    uint64_t first=counter(atomic_load_explicit(r->words+1,memory_order_seq_cst));
    if(!first||(first&1)||first<=after_serial)return VBSC_BUSY;
    uint8_t before[VBSC_SIZE],after[VBSC_SIZE];load_words(r,0,before,VBSC_SIZE);
    vbsc_control c;int result=vbsc_parse(before,VBSC_SIZE,&c);
    if(result)return result;
    if(c.begin!=first)return VBSC_BUSY;
    if(c.capacity!=m->capacity||c.generation!=m->generation||memcmp(c.namespace_id,m->namespace_id,16))return VBSC_MAPPING;
    if(c.used>cap)return VBSC_INVALID;
    load_words(r,VBSC_SIZE,p,(size_t)c.used);
    load_words(r,0,after,VBSC_SIZE);
    uint64_t last=counter(atomic_load_explicit(r->words+1,memory_order_seq_cst));
    if(first!=last)return VBSC_BUSY;
    const uint8_t *body=NULL;
    result=vbsc_validate_staging(before,VBSC_SIZE,p,(size_t)c.used,after,VBSC_SIZE,m,out,&body);
    if(result)return result;
    *used=(size_t)c.used;if(serial)*serial=first;return VBSC_OK;
}
int vbsc_atomic_snapshot(const vbsc_atomic_region *r,const vbsc_mapping *m,uint8_t *p,size_t cap,size_t *used,vbcf_header *out,vbcf_sha256_fn hash,void *ctx,uint64_t after_serial,uint64_t *serial) {
    int result=vbsc_atomic_stage(r,m,p,cap,used,out,after_serial,serial);
    if(result)return result;
    if(vbcf_validate_payload(out,p+VBCF_HEADER_SIZE,*used-VBCF_HEADER_SIZE,hash,ctx)) {
        *used=0;if(serial)*serial=0;memset(out,0,sizeof(*out));return VBSC_INVALID;
    }
    return VBSC_OK;
}
