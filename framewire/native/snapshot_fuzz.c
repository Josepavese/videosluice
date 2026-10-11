/* Test-only sanitizer entry point; input before64|packet|after64. */
#include "causal_snapshot.h"
#include <assert.h>
#include <openssl/sha.h>
#include <string.h>

static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return SHA256(p,n,out)?0:-1;}
int LLVMFuzzerTestOneInput(const uint8_t *data,size_t size) {
    vbsc_control c={0};uint8_t encoded[VBSC_SIZE];
    if(!vbsc_parse(data,size,&c)){
        assert(!vbsc_marshal(&c,encoded));
        assert(!memcmp(data,encoded,VBSC_SIZE));
    }
    if(size<2*VBSC_SIZE) return 0;
    vbsc_mapping m={0};m.namespace_id[0]=1;m.generation=UINT64_MAX;m.capacity=1024;
    vbcf_header h={0},zero={0};const uint8_t *p=NULL;
    int result=vbsc_validate_snapshot(data,VBSC_SIZE,data+VBSC_SIZE,size-2*VBSC_SIZE,
        data+size-VBSC_SIZE,VBSC_SIZE,&m,hash,NULL,&h,&p);
    if(!result){
        uint8_t header[VBCF_HEADER_SIZE];
        assert(p==data+VBSC_SIZE+VBCF_HEADER_SIZE);
        assert(!vbcf_marshal_header(&h,header));
        assert(!memcmp(header,data+VBSC_SIZE,VBCF_HEADER_SIZE));
    }else{
        assert(!p);
        assert(!memcmp(&h,&zero,sizeof(h)));
    }
    return 0;
}
