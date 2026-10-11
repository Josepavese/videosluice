#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "../atomic_snapshot.c"
int main(void) {
    for(size_t offset=0;offset<=64;offset+=8)for(size_t count=0;count<=129;count++)for(size_t align=0;align<8;align++){
        _Atomic uint64_t shared[32];for(size_t k=0;k<32;k++)atomic_init(shared+k,UINT64_MAX);
        vbsc_atomic_region region={shared,sizeof(shared)};
        size_t input_size=count+align;uint8_t *raw=malloc(input_size?input_size:1),*out=malloc(count+align+1);assert(raw&&out);
        memset(out,165,count+align+1);for(size_t i=0;i<count;i++)raw[align+i]=(uint8_t)(i*17+count);
        store_words(&region,offset,raw+align,count);
        load_words(&region,offset,out+align,count);
        assert(!memcmp(raw+align,out+align,count));
        for(size_t i=0;i<align;i++)assert(out[i]==165);
        assert(out[align+count]==165);
        for(size_t i=0;i<offset/8;i++)assert(atomic_load(shared+i)==UINT64_MAX);
        for(size_t i=(offset+count+7)/8;i<32;i++)assert(atomic_load(shared+i)==UINT64_MAX);
        if(count%8){uint64_t word=atomic_load(shared+(offset+count)/8);uint8_t bytes[8];memcpy(bytes,&word,8);
            for(size_t i=count%8;i<8;i++)assert(bytes[i]==0);}
        free(raw);free(out);
    }
    puts("ATOMIC_WORD_BOUNDARIES_PASS counts=0..129 offsets=0..64 alignments=0..7");
    return 0;
}
