/* Test executable only, excluded from provider builds: Go/native interop. */
#define _GNU_SOURCE
#include "launch.h"
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/wait.h>
#include <unistd.h>

static int close_failure_fd=-1,close_failure_calls;
int __real_close(int fd);
int __wrap_close(int fd) {
    if(fd==close_failure_fd&&!close_failure_calls) {
        close_failure_calls++;
        assert(__real_close(fd)==0);
        errno=EINTR;return -1; /* Physical release cannot be inferred by caller. */
    }
    return __real_close(fd);
}

static int digit(char c) {
    if(c>='0'&&c<='9')return c-'0';
    if(c>='a'&&c<='f')return c-'a'+10;
    return -1;
}
static size_t decode(const char *text,uint8_t *out) {
    size_t n=strlen(text);
    assert(n<=164&&n%2==0);
    for(size_t j=0;j<n/2;j++) {
        int a=digit(text[j*2]),b=digit(text[j*2+1]);
        assert(a>=0&&b>=0);out[j]=(uint8_t)(a*16+b);
    }
    return n/2;
}
static int fd_count(void) {
    DIR *dir=opendir("/proc/self/fd");assert(dir);
    int n=0;struct dirent *entry;
    while((entry=readdir(dir)))if(strcmp(entry->d_name,".")&&strcmp(entry->d_name,".."))n++;
    assert(closedir(dir)==0);return n;
}
int main(int argc,char **argv) {
    assert(argc==3);
    const char *mode=argv[1];
    uint8_t metadata[82];size_t metadata_size=decode(argv[2],metadata);
    framepeer_linux_launch launch={0};
    int before=fd_count();
    int rc=framepeer_linux_accept(&launch,3,4,metadata,metadata_size,1000);
    if(!strcmp(mode,"reject")) {
        assert(rc!=0&&launch.closed&&!launch.accepted);
        assert(framepeer_linux_close(&launch)==0);
        assert(fcntl(3,F_GETFD)<0&&errno==EBADF);
        assert(fcntl(4,F_GETFD)<0&&errno==EBADF);
        assert(fd_count()==before-2);
        printf("REJECTED code=%d stage=%s\n",rc,launch.stage);assert(fflush(stdout)==0);
        assert(getchar()!=EOF);
        puts("NATIVE_MAIN_RETURN handles_released=1");assert(fflush(stdout)==0);
        return 0;
    }
    assert(rc==0);
    assert(fd_count()==before+1); /* exactly the received self pidfd */
    assert(fcntl(launch.socket,F_GETFD)&FD_CLOEXEC);
    assert(fcntl(launch.controller_fd,F_GETFD)&FD_CLOEXEC);
    assert(fcntl(launch.self_fd,F_GETFD)&FD_CLOEXEC);
    int steady=fd_count();
    if(!strcmp(mode,"fork")) {
        pid_t child=fork();assert(child>=0);
        if(!child) {
            assert(framepeer_linux_current(&launch)==FRAMEPEER_UNAUTHORIZED);
            assert(framepeer_linux_send(&launch,(const uint8_t *)"forged",6,100)==FRAMEPEER_UNAUTHORIZED);
            _exit(0);
        }
        int status;assert(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
    }
    assert(framepeer_linux_send(&launch,(const uint8_t *)"READY",5,1000)==0);
    if(!strcmp(mode,"normal")) {
        framepeer_linux_launch delegate={0};
        assert(framepeer_linux_delegate_sender(&launch,&delegate)==0);
        assert(delegate.accepted&&delegate.socket!=launch.socket);
        assert(fd_count()==steady+3&&framepeer_linux_current(&delegate)==0);
        assert(framepeer_linux_close(&delegate)==0);
        assert(fd_count()==steady&&framepeer_linux_current(&launch)==0);
    }
    uint8_t packet[4096];size_t got=99;
    if(!strcmp(mode,"deadline")) {
        assert(framepeer_linux_receive(&launch,packet,sizeof(packet),&got,20)==FRAMEPEER_DEADLINE);
        assert(got==0&&framepeer_linux_current(&launch)==0);
        assert(framepeer_linux_send(&launch,(const uint8_t *)"TIMEOUT",7,1000)==0);
    }
    if(!strcmp(mode,"shared_deadline")) {
        int64_t until=99;
        assert(framepeer_linux_deadline_after(0,&until)==FRAMEPEER_INVALID&&until==0);
        assert(framepeer_linux_deadline_after(5001,&until)==FRAMEPEER_INVALID&&until==0);
        assert(framepeer_linux_deadline_after(1,NULL)==FRAMEPEER_INVALID);
        assert(framepeer_linux_deadline_after(100,&until)==0);
        assert(framepeer_linux_send_until(&launch,(const uint8_t *)"BUDGET",6,until)==0);
        struct timespec expired={.tv_sec=(time_t)(until/1000000000),.tv_nsec=(long)(until%1000000000)};
        int wait_result;
        do {wait_result=clock_nanosleep(CLOCK_MONOTONIC,TIMER_ABSTIME,&expired,NULL);} while(wait_result==EINTR);
        assert(wait_result==0);
        assert(framepeer_linux_send_until(&launch,(const uint8_t *)"late",4,until)==FRAMEPEER_DEADLINE);
        assert(framepeer_linux_send_until(&launch,(const uint8_t *)"invalid",7,0)==FRAMEPEER_INVALID);
        assert(framepeer_linux_send_until(&launch,(const uint8_t *)"invalid",7,INT64_MAX)==FRAMEPEER_INVALID);
        assert(framepeer_linux_send(&launch,(const uint8_t *)"TIMEOUT",7,1000)==0);
    }
    if(!strcmp(mode,"rights")||!strcmp(mode,"truncated")||!strcmp(mode,"foreign_sender")) {
        rc=framepeer_linux_receive(&launch,packet,sizeof(packet),&got,1000);
        assert(rc==(!strcmp(mode,"foreign_sender")?FRAMEPEER_UNAUTHORIZED:FRAMEPEER_RECORD));
        assert(got==0&&fd_count()==steady);
        assert(framepeer_linux_send(&launch,(const uint8_t *)"REJECTED_RECORD",15,1000)==0);
    }
    assert(framepeer_linux_receive(&launch,packet,sizeof(packet),&got,1000)==0);
    assert(got==4&&!memcmp(packet,"ping",4));
    assert(framepeer_linux_send(&launch,packet,got,1000)==0);
    assert(framepeer_linux_receive(&launch,packet,sizeof(packet),&got,1000)==0);
    assert(got==4&&!memcmp(packet,"done",4));
    int close_expected=0;
    if(!strcmp(mode,"close_failure")){close_failure_fd=launch.socket;close_expected=FRAMEPEER_UNAVAILABLE;}
    assert(framepeer_linux_close(&launch)==close_expected);
    int reused=open("/dev/null",O_RDONLY|O_CLOEXEC);assert(reused>=0);
    assert(framepeer_linux_close(&launch)==close_expected&&fcntl(reused,F_GETFD)>=0);
    if(close_expected)assert(close_failure_calls==1&&launch.close_result==FRAMEPEER_UNAVAILABLE);
    assert(framepeer_linux_send(&launch,packet,1,100)==FRAMEPEER_RETIRED);
    assert(framepeer_linux_receive(&launch,packet,sizeof(packet),&got,100)==FRAMEPEER_RETIRED&&got==0);
    assert(framepeer_linux_accept(&launch,reused,STDIN_FILENO,metadata,metadata_size,100)==FRAMEPEER_INVALID);
    assert(fcntl(reused,F_GETFD)>=0);assert(close(reused)==0);
    assert(fd_count()==before-2);
    puts("NATIVE_LAUNCH_PASS handles_released=1");assert(fflush(stdout)==0);
    /* Remain alive until the parent has observed its final write outcome. */
    assert(getchar()!=EOF);
    puts("NATIVE_MAIN_RETURN handles_released=1");assert(fflush(stdout)==0);
    return 0;
}
