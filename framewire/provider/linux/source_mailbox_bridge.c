//go:build linux && cgo

#include "native/source_mailbox.c"
/* Compile guarded producer and staged consumer in the same mailbox unit. */
int vbsm_go_stage(vbsm_source_mailbox *o,unsigned slot,uint64_t after,
    uint8_t *p,size_t cap,size_t *used,uint64_t *serial) {
    return vbsm_source_mailbox_stage(o,slot,after,p,cap,used,serial);
}
