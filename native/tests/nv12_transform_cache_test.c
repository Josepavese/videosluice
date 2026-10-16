/* Geometry-only cache: exact legacy digest, no per-frame crypto on a hit. */
#include "nv12_causal_transform.h"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>

static unsigned hash_calls;
static int hash(void *ctx,const uint8_t *data,size_t size,uint8_t digest[32]) {
    (void)ctx;hash_calls++;
    return SHA256(data,size,digest)?0:-1;
}
static int failed_hash(void *ctx,const uint8_t *data,size_t size,uint8_t digest[32]) {
    (void)ctx;(void)data;(void)size;memset(digest,0xaa,32);return -1;
}
static void zero(const uint8_t digest[32]) {
    for(unsigned i=0;i<32;i++)assert(digest[i]==0);
}
static void check(video_broker_nv12_transform_cache *cache,const int key[7]) {
    uint8_t expected[32],actual[32];
    assert(!video_broker_nv12_transform_digest(key[0],key[1],key[2],key[3],key[4],key[5],key[6],hash,NULL,expected));
    unsigned before=hash_calls;
    assert(!video_broker_nv12_transform_digest_cached(cache,key[0],key[1],key[2],key[3],key[4],key[5],key[6],hash,NULL,actual));
    assert(hash_calls==before+1&&!memcmp(actual,expected,32));
    memset(actual,0xee,32); /* Caller mutation cannot change cached bytes. */
    for(unsigned frame=0;frame<1000;frame++) {
        assert(!video_broker_nv12_transform_digest_cached(cache,key[0],key[1],key[2],key[3],key[4],key[5],key[6],hash,NULL,actual));
        assert(hash_calls==before+1&&!memcmp(actual,expected,32));
    }
}
int main(void) {
    const int initial[]={1920,1080,1984,640,360,0,0};
    video_broker_nv12_transform_cache cache={0},other={0};
    check(&cache,initial);
    /* Every geometry input invalidates independently, even if two layouts
     * happen to have the same canonical mathematical digest. */
    for(unsigned field=0;field<7;field++) {
        int key[7];memcpy(key,initial,sizeof(key));
        key[field]+=field<5?2:1;check(&cache,key);check(&cache,initial);
    }
    for(int orientation=0;orientation<4;orientation++)for(int policy=0;policy<3;policy++) {
        int key[]={1280,720,1344,640,480,orientation,policy};check(&other,key);
    }
    uint8_t actual[32];
    assert(video_broker_nv12_transform_digest_cached(&cache,1921,1080,1920,640,360,0,0,hash,NULL,actual)==-1);
    zero(actual);assert(!cache.valid);
    check(&cache,initial);
    assert(video_broker_nv12_transform_digest_cached(&cache,1920,1080,1920,640,360,0,0,failed_hash,NULL,actual)==-1);
    zero(actual);assert(!cache.valid);
    check(&cache,initial);
    /* A new provider context is not allowed to reuse old provider evidence. */
    unsigned before=hash_calls;
    assert(!video_broker_nv12_transform_digest_cached(&cache,1920,1080,1920,640,360,0,0,hash,&cache,actual));
    assert(hash_calls==before+1);
    assert(video_broker_nv12_transform_digest_cached(NULL,1920,1080,1920,640,360,0,0,hash,NULL,actual)==-1);zero(actual);
    assert(video_broker_nv12_transform_digest_cached(&cache,1920,1080,1920,640,360,0,0,NULL,NULL,actual)==-1);zero(actual);
    assert(video_broker_nv12_transform_digest_cached(&cache,1920,1080,1920,640,360,0,0,hash,NULL,NULL)==-1);
    puts("OK_NV12_TRANSFORM_CACHE_EXACT_BOUNDED_INVALIDATION");
    return 0;
}
