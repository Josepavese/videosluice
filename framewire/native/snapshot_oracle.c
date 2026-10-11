/* Test transport/hash provider only, not production filesystem or SHM code. */
#include "causal_snapshot.h"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <string.h>

static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return SHA256(p,n,out)?0:-1;}
static int digit(char c){if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;return -1;}
static int decode(const char *s,size_t len,uint8_t *out,size_t capacity) {
    if(len%2||len/2>capacity) return -1;
    for(size_t i=0;i<len/2;i++){
        int h=digit(s[2*i]),l=digit(s[2*i+1]);
        if(h<0||l<0) return -1;
        out[i]=(uint8_t)(h*16+l);
    }
    return 0;
}
int main(int argc,char **argv) {
    if(argc==2&&!strcmp(argv[1],"--controls")) {
        const uint64_t lengths[]={472,472,454,448,453};
        for(size_t i=0;i<5;i++){
            vbsc_control c={0};c.begin=UINT64_MAX-1;c.end=c.begin;c.capacity=1024;c.used=lengths[i];c.generation=UINT64_MAX;c.namespace_id[0]=1;
            uint8_t b[VBSC_SIZE];assert(!vbsc_marshal(&c,b));
            for(size_t j=0;j<sizeof(b);j++) printf("%02x",b[j]);
            putchar('\n');
        }
        return 0;
    }
    char line[8192];uint8_t b[256],p[2048],a[256];
    vbsc_mapping m={0};m.namespace_id[0]=1;m.generation=UINT64_MAX;m.capacity=1024;
    while(fgets(line,sizeof(line),stdin)) {
        char *first=strchr(line,':');char *second=first?strchr(first+1,':'):NULL;
        if(!first||!second){puts("0");continue;}
        size_t bl=(size_t)(first-line),pl=(size_t)(second-first-1),al=strcspn(second+1,"\r\n");
        if(decode(line,bl,b,sizeof(b))||decode(first+1,pl,p,sizeof(p))||decode(second+1,al,a,sizeof(a))){puts("0");continue;}
        vbcf_header h={0};const uint8_t *body=NULL;
        int result=vbsc_validate_snapshot(b,bl/2,p,pl/2,a,al/2,&m,hash,NULL,&h,&body);
        if(!result) {
            assert(body==p+VBCF_HEADER_SIZE);
            assert(vbsc_validate_snapshot(b,bl/2,p,pl/2,a,al/2,&m,NULL,NULL,&h,&body));
            assert(!body);
        }
        puts(result?"0":"1");
    }
    return ferror(stdin)?1:0;
}
