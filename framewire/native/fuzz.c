#include "causal_envelope.h"
#include <assert.h>
#include <openssl/sha.h>
#include <string.h>

static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){
 (void)ctx;return SHA256(p,n,out)?0:-1;
}
int LLVMFuzzerTestOneInput(const uint8_t *data,size_t size){
 vbcf_header h={0};uint8_t roundtrip[VBCF_HEADER_SIZE];
 /* Check all invalid header lengths too, without accessing undersized input. */
 if(size<VBCF_HEADER_SIZE){assert(vbcf_parse_header(data,size,&h));return 0;}
 if(!vbcf_parse_header(data,VBCF_HEADER_SIZE,&h)){
  assert(!vbcf_marshal_header(&h,roundtrip));assert(!memcmp(data,roundtrip,VBCF_HEADER_SIZE));
  (void)vbcf_validate_payload(&h,data+VBCF_HEADER_SIZE,size-VBCF_HEADER_SIZE,hash,NULL);
 }
 return 0;
}
