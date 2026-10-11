#define _GNU_SOURCE
/* Same-process access below is test-only, to inspect kernel protections and
 * inject a torn writer. Production readers never receive this writable view. */
#include "source_mailbox.c"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
static unsigned hashes;
static int hash(void *unused,const uint8_t *p,size_t n,uint8_t out[32]) {
    (void)unused;hashes++;return SHA256(p,n,out)?0:-1;
}
int main(void) {
    vbsc_mapping mapping={{1},23,VBCF_HEADER_SIZE+24};int error;
    vbsm_source_mailbox *reader=vbsm_source_mailbox_create(&mapping,&error);
    assert(reader&&!error&&!reader->memory);
    assert(vbsm_source_mailbox_confirm(reader)==VBSC_INVALID);
    int fd=vbsm_source_mailbox_export(reader);assert(fd>=3);
    vbsm_source_mailbox *writer=vbsm_source_mailbox_attach(fd,&mapping,&error);
    assert(writer&&!error);assert(!vbsm_source_mailbox_confirm(reader));
    assert(mprotect(reader->memory,reader->bytes,PROT_READ|PROT_WRITE)==-1);
    assert(mmap(NULL,reader->bytes,PROT_READ|PROT_WRITE,MAP_SHARED,reader->fd,0)==MAP_FAILED);
    assert(ftruncate(reader->fd,0)==-1&&ftruncate(reader->fd,(off_t)reader->bytes+1)==-1);
    assert(pwrite(reader->fd,"x",1,0)==-1);
    uint8_t copy[VBCF_HEADER_SIZE+24],pixels[24];size_t used=123;uint64_t serial=123;
    assert(vbsm_source_mailbox_stage(reader,0,0,copy,sizeof(copy),&used,&serial)==VBSC_BUSY);
    assert(!used&&!serial&&!hashes);
    vbcf_header h={0};h.kind=1;h.codec=3;h.surface=1;h.correlation=1;
    h.width=4;h.height=2;h.stride_y=h.stride_uv=8;h.payload_bytes=24;
    h.source_epoch=1;h.namespace_id[0]=1;h.graph[0]=1;h.transform[0]=1;
    strcpy(h.source,"front");strcpy(h.process,"broker-1");
    /* No reader for 1000 publications: two fixed slots, no wait or queue. */
    for(uint64_t sequence=1;sequence<=1000;sequence++) {
        h.origin_sequence=sequence;memset(pixels,(int)(sequence%251),sizeof(pixels));
        unsigned before=hashes;
        assert(!vbsm_source_mailbox_publish(writer,(unsigned)(sequence%2),&h,pixels,sizeof(pixels),hash,NULL));
        assert(hashes==before+1);
    }
    for(unsigned slot=0;slot<2;slot++) {
        unsigned before_stage=hashes;
        assert(!vbsm_source_mailbox_stage(reader,slot,0,copy,sizeof(copy),&used,&serial));
        assert(hashes==before_stage);
        assert(used==sizeof(copy)&&serial==1000);
        vbcf_header read;assert(!vbcf_parse_header(copy,VBCF_HEADER_SIZE,&read));
        assert(!vbcf_validate_payload(&read,copy+VBCF_HEADER_SIZE,used-VBCF_HEADER_SIZE,hash,NULL));
        assert(read.origin_sequence==1000-slot);
        for(size_t i=VBCF_HEADER_SIZE;i<used;i++)assert(copy[i]==(uint8_t)(read.origin_sequence%251));
        unsigned before=hashes;
        assert(vbsm_source_mailbox_stage(reader,slot,serial,copy,sizeof(copy),&used,&serial)==VBSC_BUSY);
        assert(!used&&!serial&&hashes==before);
    }
    /* Staging retains the expected SHA, never recomputes it over corruption.
     * The mandatory pre-extraction check (also integration-tested in Go)
     * must reject corruption without a new serial, even outside a crop. */
    atomic_fetch_xor(writer->slots[0].words+(VBSC_SIZE+VBCF_HEADER_SIZE)/8,UINT64_C(1));
    unsigned before_stage=hashes;
    assert(!vbsm_source_mailbox_stage(reader,0,0,copy,sizeof(copy),&used,&serial));
    assert(hashes==before_stage);
    vbcf_header corrupt;assert(!vbcf_parse_header(copy,VBCF_HEADER_SIZE,&corrupt));
    assert(vbcf_validate_payload(&corrupt,copy+VBCF_HEADER_SIZE,used-VBCF_HEADER_SIZE,hash,NULL)!=0);
    assert(!vbsm_source_mailbox_close(writer));assert(!vbsm_source_mailbox_close(reader));
    puts("SOURCE_MAILBOX_PASS slots=2 publications=1000 producer_hash=1 staging_hash=0 readonly_sealed=1 unchanged_no_copy=1 corruption_rejected_before_pixels=1");
    return 0;
}
