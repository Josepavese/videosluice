#include "pipeline_latency.h"
#include <assert.h>
#include <stdio.h>
int main(void) {
    vb_pipeline_latency p={0}; vb_pipeline_delay d={0};
    assert(!vb_pipeline_checkpoint(&p,"authority",100,10,&d));
    assert(!vb_pipeline_checkpoint(&p,"detach",110,20,&d));
    assert(vb_pipeline_checkpoint(&p,"decode",6472680,50,&d));
    assert(d.wall_us==6472570 && d.cpu_us==30 && !d.suppressed);
    assert(d.phase[0]=='d');
    assert(!vb_pipeline_checkpoint(&p,"publish",6672680,60,&d));
    assert(vb_pipeline_checkpoint(&p,"analysis",7672680,80,&d));
    assert(d.wall_us==1000000 && d.cpu_us==20 && d.suppressed==1);
    assert(!vb_pipeline_checkpoint(&p,"clock_regression",1,1,&d));
    puts("OK bounded pipeline phase latency wall/thread CPU");
    return 0;
}
