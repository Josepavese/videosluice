/* Test-only executable. OpenSSL implements the injected hash port here. */
#include "causal_envelope.h"
#include <assert.h>
#include <limits.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return SHA256(p,n,out)?0:-1;}
static int failed_hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;(void)p;(void)n;(void)out;return -1;}
static void hex(const uint8_t *p,size_t n){for(size_t i=0;i<n;i++)printf("%02x",p[i]);}
static int digit(char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;return -1;}
static vbcf_header base(void){
 vbcf_header h={0};h.kind=2;h.codec=3;h.surface=3;h.correlation=1;h.flags=3;
 h.width=4;h.height=2;h.stride_y=8;h.stride_uv=8;h.source_epoch=UINT64_MAX-3;
 h.origin_sequence=UINT64_MAX-2;h.publication_epoch=UINT64_MAX-1;h.delivery_id=UINT64_MAX;
 h.capture_ns=123456;h.media_pts=-2345;h.namespace_id[0]=1;
 hash(NULL,(const uint8_t *)"graph",5,h.graph);hash(NULL,(const uint8_t *)"transform",9,h.transform);
 strcpy(h.source,"front");strcpy(h.stream,"analysis");strcpy(h.process,"broker-1");return h;
}
static void vector(vbcf_header h,const uint8_t *p,size_t n){
 h.payload_bytes=(uint32_t)n;assert(!hash(NULL,p,n,h.payload_sha));
 uint8_t b[VBCF_HEADER_SIZE];assert(!vbcf_marshal_header(&h,b));
 assert(!vbcf_validate_payload(&h,p,n,hash,NULL));
 assert(vbcf_validate_payload(&h,p,n,NULL,NULL));assert(vbcf_validate_payload(&h,p,n,failed_hash,NULL));
 hex(b,sizeof(b));hex(p,n);putchar('\n');
}
static void vectors(void){
 uint8_t p[24];for(size_t i=0;i<24;i++)p[i]=(uint8_t)i;vbcf_header h=base();vector(h,p,24);
 h.kind=1;h.surface=2;h.stream[0]=0;h.publication_epoch=0;h.delivery_id=0;vector(h,p,24);
 h=base();h.codec=2;h.stride_y=0;h.stride_uv=0;h.origin_sequence=0;h.correlation=0;h.flags=30;h.capture_ns=0;
 const uint8_t h265[]={0,0,0,1,64,1};vector(h,h265,sizeof(h265));
 h=base();h.kind=3;h.codec=0;h.surface=0;h.correlation=0;h.flags=0;h.width=0;h.height=0;h.stride_y=0;h.stride_uv=0;h.origin_sequence=0;h.delivery_id=0;h.capture_ns=0;h.media_pts=0;memset(h.transform,0,32);vector(h,NULL,0);
 h=base();h.codec=1;h.stride_y=0;h.stride_uv=0;h.flags=7;h.media_pts=INT64_MIN;memset(h.source,'S',64);memset(h.stream,'T',64);memset(h.process,'P',64);
 const uint8_t h264[]={0,0,0,1,103};vector(h,h264,sizeof(h264));
 h=base();h.kind=5;h.output_sequence=UINT64_MAX;h.publication_epoch=0;h.delivery_id=0;vector(h,p,24);
 h.correlation=0;h.origin_sequence=0;h.capture_ns=0;h.flags=2;vector(h,p,24);
}
int main(int argc,char **argv){
 if(argc==2&&!strcmp(argv[1],"--vectors")){vectors();return 0;}
 /* Deliberately bounded test transport; not a runtime reader implementation. */
 char line[4098];uint8_t b[2048];
 while(fgets(line,sizeof(line),stdin)){
  size_t chars=strcspn(line,"\r\n");int valid=chars%2==0&&chars/2<=sizeof(b);
  for(size_t i=0;valid&&i<chars/2;i++){int hi=digit(line[2*i]),lo=digit(line[2*i+1]);if(hi<0||lo<0){valid=0;break;}b[i]=(uint8_t)(hi*16+lo);}
  size_t n=chars/2;vbcf_header h={0};uint8_t roundtrip[VBCF_HEADER_SIZE];
  if(!valid||n<VBCF_HEADER_SIZE||vbcf_parse_header(b,VBCF_HEADER_SIZE,&h)||vbcf_validate_payload(&h,b+VBCF_HEADER_SIZE,n-VBCF_HEADER_SIZE,hash,NULL)||vbcf_marshal_header(&h,roundtrip)){puts("0");continue;}
  assert(!memcmp(b,roundtrip,VBCF_HEADER_SIZE));puts("1");
 }
 return ferror(stdin)?1:0;
}
