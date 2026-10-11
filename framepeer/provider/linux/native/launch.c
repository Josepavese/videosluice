#define _GNU_SOURCE
#include "launch.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum { MAX_RECORD=16384, MAX_FDINFO=4096 };

static int failure(framepeer_linux_launch *l,const char *stage,int code) {
    l->stage=stage;
    return code;
}
static uint32_t get32(const uint8_t *p) {
    return (uint32_t)p[0]<<24|(uint32_t)p[1]<<16|(uint32_t)p[2]<<8|p[3];
}
static int identity(const uint8_t *p,size_t n,framepeer_linux_identity *out) {
    if(!p||n<19||n>82||memcmp(p,"LPFD",4)||p[4]!=1||p[17]!=n-18)return FRAMEPEER_INVALID;
    uint32_t pid=get32(p+5);
    if(!pid||pid>INT_MAX)return FRAMEPEER_INVALID;
    framepeer_linux_identity v={.pid=(int)pid,.uid=get32(p+9),.gid=get32(p+13)};
    for(size_t j=18;j<n;j++) {
        uint8_t c=p[j];
        if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='.'||c=='-'||c=='_'))return FRAMEPEER_INVALID;
        v.generation[j-18]=(char)c;
    }
    *out=v;
    return 0;
}
static int now_ns(int64_t *out) {
    struct timespec t;
    if(clock_gettime(CLOCK_MONOTONIC,&t)||t.tv_sec<0||t.tv_sec>INT64_MAX/1000000000)return FRAMEPEER_UNAVAILABLE;
    *out=(int64_t)t.tv_sec*1000000000+t.tv_nsec;
    return 0;
}
static int deadline(int timeout_ms,int64_t *out) {
    int64_t now;
    if(timeout_ms<1||timeout_ms>5000)return FRAMEPEER_INVALID;
    if(now_ns(&now)||now>INT64_MAX-(int64_t)timeout_ms*1000000)return FRAMEPEER_UNAVAILABLE;
    *out=now+(int64_t)timeout_ms*1000000;
    return 0;
}
int framepeer_linux_deadline_after(int timeout_ms,int64_t *out) {
    if(!out)return FRAMEPEER_INVALID;
    *out=0;
    return deadline(timeout_ms,out);
}
static int live_fd(int fd) {
    struct pollfd p={.fd=fd,.events=POLLIN};
    for(int j=0;j<4;j++) {
        int n=poll(&p,1,0);
        if(n<0&&errno==EINTR)continue;
        if(n<0)return FRAMEPEER_UNAVAILABLE;
        return n||p.revents?FRAMEPEER_RETIRED:0;
    }
    return FRAMEPEER_UNAVAILABLE;
}
static int cloexec(int fd) {
    int flags=fcntl(fd,F_GETFD);
    return flags<0||fcntl(fd,F_SETFD,flags|FD_CLOEXEC)<0?FRAMEPEER_UNAVAILABLE:0;
}
static int close_one(framepeer_linux_launch *l,int *fd) {
    if(*fd<0)return 0;
    int owned=*fd;
    *fd=-1; /* Never retry close against a possibly reused descriptor number. */
    if(close(owned))l->close_result=FRAMEPEER_UNAVAILABLE;
    return l->close_result;
}
int framepeer_linux_close(framepeer_linux_launch *l) {
    if(!l||!l->initialized)return FRAMEPEER_INVALID;
    if(l->closed)return l->close_result;
    l->closed=true;l->accepted=false;
    (void)close_one(l,&l->socket);
    (void)close_one(l,&l->controller_fd);
    (void)close_one(l,&l->self_fd);
    return l->close_result;
}
static int inspect_pidfd(framepeer_linux_launch *l,int fd,int expected) {
    char path[64],link[64],info[MAX_FDINFO+1];
    if(cloexec(fd))return failure(l,"capability_cloexec",FRAMEPEER_UNAVAILABLE);
    int n=snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    if(n<0||(size_t)n>=sizeof(path))return failure(l,"capability_descriptor",FRAMEPEER_INVALID);
    ssize_t length=readlink(path,link,sizeof(link));
    static const char wanted[]="anon_inode:[pidfd]";
    if(length<0)return failure(l,"capability_descriptor",FRAMEPEER_UNAVAILABLE);
    if(length!=(ssize_t)(sizeof(wanted)-1)||memcmp(link,wanted,sizeof(wanted)-1))return failure(l,"capability_type",FRAMEPEER_INVALID);
    n=snprintf(path,sizeof(path),"/proc/self/fdinfo/%d",fd);
    if(n<0||(size_t)n>=sizeof(path))return failure(l,"capability_fdinfo",FRAMEPEER_INVALID);
    int input=open(path,O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
    if(input<0)return failure(l,"capability_fdinfo",FRAMEPEER_UNAVAILABLE);
    size_t used=0;
    bool eof=false;
    /* procfs is bounded; also bound syscalls under EINTR/short-read storms. */
    for(int attempt=0;attempt<16&&used<sizeof(info);attempt++) {
        ssize_t got=read(input,info+used,sizeof(info)-used);
        if(got<0&&errno==EINTR)continue;
        if(got<0)break;
        if(!got){eof=true;break;}
        used+=(size_t)got;
    }
    if(close(input)){l->close_result=FRAMEPEER_UNAVAILABLE;return failure(l,"capability_fdinfo_close",FRAMEPEER_UNAVAILABLE);}
    if(!eof||used>MAX_FDINFO)return failure(l,"capability_fdinfo",FRAMEPEER_UNAVAILABLE);
    bool found=false;
    for(size_t at=0;at<used;) {
        size_t end=at;
        while(end<used&&info[end]!='\n')end++;
        if(end-at>=4&&!memcmp(info+at,"Pid:",4)) {
            if(found)return failure(l,"capability_pid_duplicate",FRAMEPEER_INVALID);
            found=true;
            size_t p=at+4;
            while(p<end&&(info[p]==' '||info[p]=='\t'))p++;
            if(p<end&&info[p]=='-')return failure(l,"capability_retired",FRAMEPEER_RETIRED);
            uint64_t value=0;size_t digits=0;
            while(p<end&&info[p]>='0'&&info[p]<='9') {
                value=value*10+(unsigned)(info[p++]-'0');digits++;
                if(value>INT_MAX)return failure(l,"capability_pid",FRAMEPEER_INVALID);
            }
            while(p<end&&(info[p]==' '||info[p]=='\t'))p++;
            if(!digits||p!=end||value!=(uint64_t)expected)return failure(l,"capability_pid",FRAMEPEER_UNAUTHORIZED);
        }
        at=end+1;
    }
    if(!found)return failure(l,"capability_fdinfo",FRAMEPEER_UNAVAILABLE);
    int result=live_fd(fd);
    return result?failure(l,"capability_liveness",result):0;
}
int framepeer_linux_current(framepeer_linux_launch *l) {
    if(!l||!l->initialized)return FRAMEPEER_INVALID;
    if(l->closed)return failure(l,"launch_closed",FRAMEPEER_RETIRED);
    if(l->close_result)return failure(l,"native_release_unconfirmed",l->close_result);
    if(l->accepted&&(l->self.pid!=getpid()||l->self.uid!=(uint32_t)getuid()||l->self.gid!=(uint32_t)getgid()))return failure(l,"self_execution_changed",FRAMEPEER_UNAUTHORIZED);
    int result=live_fd(l->controller_fd);
    if(result)return failure(l,"controller_retired",result);
    if(l->accepted&&(result=live_fd(l->self_fd)))return failure(l,"self_retired",result);
    return 0;
}
static int wait_socket(framepeer_linux_launch *l,short event,int64_t until) {
    for(;;) {
        int result=framepeer_linux_current(l);
        if(result)return result;
        int64_t now;
        if(now_ns(&now))return failure(l,"io_clock",FRAMEPEER_UNAVAILABLE);
        if(now>=until)return failure(l,"io_deadline",FRAMEPEER_DEADLINE);
        struct pollfd p[3]={{.fd=l->socket,.events=event},{.fd=l->controller_fd,.events=POLLIN},{.fd=l->accepted?l->self_fd:-1,.events=POLLIN}};
        int n=poll(p,3,(int)((until-now+999999)/1000000));
        if(n<0&&errno==EINTR)continue;
        if(n<0)return failure(l,"io_poll",FRAMEPEER_UNAVAILABLE);
        if(p[1].revents||p[2].revents)return failure(l,"io_execution_retired",FRAMEPEER_RETIRED);
        if(p[0].revents&event)return 0;
        if(p[0].revents)return failure(l,"io_transport_closed",FRAMEPEER_UNAVAILABLE);
    }
}
static int receive_record(framepeer_linux_launch *l,uint8_t *p,size_t capacity,size_t *count,int *capability,int64_t until) {
    *count=0;
    if(capability)*capability=-1;
    for(;;) {
        int result=wait_socket(l,POLLIN,until);
        if(result)return result;
        /* Kernel closes excess rights on MSG_CTRUNC. Capture at most one
         * intended capability and dispose every descriptor actually received. */
        union {struct cmsghdr align;uint8_t bytes[CMSG_SPACE(sizeof(struct ucred))+CMSG_SPACE(sizeof(int))];} control;
        struct iovec iov={.iov_base=p,.iov_len=capacity};
        struct msghdr message={.msg_iov=&iov,.msg_iovlen=1,.msg_control=control.bytes,.msg_controllen=sizeof(control.bytes)};
        ssize_t n=recvmsg(l->socket,&message,MSG_DONTWAIT|MSG_CMSG_CLOEXEC);
        if(n<0&&(errno==EINTR||errno==EAGAIN||errno==EWOULDBLOCK))continue;
        if(n<0)return failure(l,"receive",FRAMEPEER_UNAVAILABLE);
        int owned[2]={-1,-1};size_t rights=0;
        struct ucred credential={0};bool has_credential=false,valid=true;
        for(struct cmsghdr *c=CMSG_FIRSTHDR(&message);c;c=CMSG_NXTHDR(&message,c)) {
            size_t offset=(size_t)((uint8_t *)c-control.bytes);
            if(message.msg_controllen>sizeof(control.bytes)||offset>message.msg_controllen||
               c->cmsg_len<CMSG_LEN(0)||c->cmsg_len>message.msg_controllen-offset){valid=false;break;}
            size_t size=c->cmsg_len-CMSG_LEN(0);
            if(c->cmsg_level==SOL_SOCKET&&c->cmsg_type==SCM_RIGHTS) {
                if(size%sizeof(int))valid=false;
                for(size_t at=0;at+sizeof(int)<=size;at+=sizeof(int)) {
                    int fd;memcpy(&fd,CMSG_DATA(c)+at,sizeof(fd));
                    if(rights<2)owned[rights]=fd;
                    else {if(close(fd))l->close_result=FRAMEPEER_UNAVAILABLE;valid=false;}
                    rights++;
                }
            } else if(c->cmsg_level==SOL_SOCKET&&c->cmsg_type==SCM_CREDENTIALS&&size==sizeof(credential)&&!has_credential) {
                memcpy(&credential,CMSG_DATA(c),sizeof(credential));has_credential=true;
            } else valid=false;
        }
        if(message.msg_flags&(MSG_TRUNC|MSG_CTRUNC))result=failure(l,"receive_truncated",FRAMEPEER_RECORD);
        else if(!valid||!has_credential||rights>(capability?1u:0u)||n<=0)result=failure(l,"receive_record",FRAMEPEER_RECORD);
        else if(credential.pid!=l->controller.pid||credential.uid!=l->controller.uid||credential.gid!=l->controller.gid)result=failure(l,"receive_sender_mismatch",FRAMEPEER_UNAUTHORIZED);
        else result=framepeer_linux_current(l);
        int64_t now;
        if(!result&&(now_ns(&now)||now>=until))result=failure(l,"receive_deadline",FRAMEPEER_DEADLINE);
        if(!result&&capability&&rights==1){*capability=owned[0];owned[0]=-1;}
        for(size_t j=0;j<2;j++)(void)close_one(l,&owned[j]);
        if(l->close_result&&!result)result=failure(l,"receive_capability_close",l->close_result);
        if(result)return result;
        *count=(size_t)n;
        return 0;
    }
}
int framepeer_linux_accept(framepeer_linux_launch *l,int socket,int controller_fd,const uint8_t *metadata,size_t metadata_size,int timeout_ms) {
    if(!l||l->initialized||socket<0||controller_fd<0||socket==controller_fd)return FRAMEPEER_INVALID;
    l->initialized=true;l->socket=socket;l->controller_fd=controller_fd;l->self_fd=-1;
    int64_t until;
    int result=deadline(timeout_ms,&until);
    if(result){failure(l,"launch_deadline",result);goto reject;}
    result=identity(metadata,metadata_size,&l->controller);
    if(result){failure(l,"launch_controller_metadata",result);goto reject;}
    result=inspect_pidfd(l,controller_fd,l->controller.pid);
    if(result)goto reject;
    int kind=0,domain=0,passcred=0;socklen_t size=sizeof(int);
    if(cloexec(socket)||getsockopt(socket,SOL_SOCKET,SO_TYPE,&kind,&size)||kind!=SOCK_SEQPACKET||
       getsockopt(socket,SOL_SOCKET,SO_DOMAIN,&domain,&size)||domain!=AF_UNIX||
       getsockopt(socket,SOL_SOCKET,SO_PASSCRED,&passcred,&size)||passcred!=1) {
        result=failure(l,"launch_socket",FRAMEPEER_INVALID);goto reject;
    }
    uint8_t packet[82];size_t received;
    result=receive_record(l,packet,sizeof(packet),&received,&l->self_fd,until);
    if(result)goto reject;
    if(l->self_fd<0){result=failure(l,"launch_self_missing",FRAMEPEER_INVALID);goto reject;}
    result=identity(packet,received,&l->self);
    if(result){failure(l,"launch_self_metadata",result);goto reject;}
    if(l->self.pid!=getpid()||l->self.uid!=(uint32_t)getuid()||l->self.gid!=(uint32_t)getgid()) {
        result=failure(l,"launch_self_identity",FRAMEPEER_UNAUTHORIZED);goto reject;
    }
    result=inspect_pidfd(l,l->self_fd,l->self.pid);
    if(result)goto reject;
    l->accepted=true;
    result=framepeer_linux_current(l);
    if(result)goto reject;
    l->stage="launch_accepted";
    return 0;
reject:
    (void)framepeer_linux_close(l);
    return result;
}
int framepeer_linux_receive(framepeer_linux_launch *l,uint8_t *buffer,size_t capacity,size_t *received,int timeout_ms) {
    if(received)*received=0;
    if(!l||!l->initialized||!buffer||!received||!capacity||capacity>MAX_RECORD)return FRAMEPEER_INVALID;
    if(l->closed)return failure(l,"launch_closed",FRAMEPEER_RETIRED);
    if(!l->accepted)return FRAMEPEER_INVALID;
    int64_t until;int result=deadline(timeout_ms,&until);
    if(result)return failure(l,"receive_contract",result);
    return framepeer_linux_receive_until(l,buffer,capacity,received,until);
}
int framepeer_linux_receive_until(framepeer_linux_launch *l,uint8_t *buffer,size_t capacity,size_t *received,int64_t until) {
    if(received)*received=0;
    if(!l||!l->initialized||!buffer||!received||!capacity||capacity>MAX_RECORD)return FRAMEPEER_INVALID;
    if(l->closed)return failure(l,"launch_closed",FRAMEPEER_RETIRED);
    if(!l->accepted)return FRAMEPEER_INVALID;
    int64_t now;
    if(now_ns(&now))return failure(l,"receive_clock",FRAMEPEER_UNAVAILABLE);
    if(until<=0||until-now>INT64_C(5000000000))return failure(l,"receive_contract",FRAMEPEER_INVALID);
    return receive_record(l,buffer,capacity,received,NULL,until);
}
int framepeer_linux_send(framepeer_linux_launch *l,const uint8_t *packet,size_t size,int timeout_ms) {
    int64_t until;int result=deadline(timeout_ms,&until);
    if(result)return l?failure(l,"send_contract",result):result;
    return framepeer_linux_send_until(l,packet,size,until);
}
int framepeer_linux_send_until(framepeer_linux_launch *l,const uint8_t *packet,size_t size,int64_t until) {
    if(!l||!l->initialized||!packet||!size||size>MAX_RECORD)return FRAMEPEER_INVALID;
    if(l->closed)return failure(l,"launch_closed",FRAMEPEER_RETIRED);
    if(!l->accepted)return FRAMEPEER_INVALID;
    int64_t now;
    if(now_ns(&now))return failure(l,"send_clock",FRAMEPEER_UNAVAILABLE);
    if(until<=0||until-now>INT64_C(5000000000))return failure(l,"send_contract",FRAMEPEER_INVALID);
    int result;
    for(;;) {
        result=wait_socket(l,POLLOUT,until);
        if(result)return result;
        ssize_t n=send(l->socket,packet,size,MSG_DONTWAIT|MSG_NOSIGNAL);
        if(n<0&&(errno==EINTR||errno==EAGAIN||errno==EWOULDBLOCK))continue;
        if(n<0||n!=(ssize_t)size)return failure(l,"send_record",FRAMEPEER_UNAVAILABLE);
        result=framepeer_linux_current(l);
        if(result)return result; /* Delivery may already have happened. */
        int64_t now;
        if(now_ns(&now)||now>=until)return failure(l,"send_deadline",FRAMEPEER_DEADLINE);
        return 0;
    }
}
int framepeer_linux_delegate_sender(framepeer_linux_launch *owner,framepeer_linux_launch *delegate) {
    if(!owner||!delegate||owner==delegate||delegate->initialized||!owner->accepted||owner->closed)
        return FRAMEPEER_INVALID;
    int result=framepeer_linux_current(owner);
    if(result)return result;
    memset(delegate,0,sizeof(*delegate));
    delegate->initialized=true;
    delegate->socket=delegate->controller_fd=delegate->self_fd=-1;
    delegate->stage="sender_delegate";
    delegate->socket=fcntl(owner->socket,F_DUPFD_CLOEXEC,3);
    if(delegate->socket<0)goto failed;
    delegate->controller_fd=fcntl(owner->controller_fd,F_DUPFD_CLOEXEC,3);
    if(delegate->controller_fd<0)goto failed;
    delegate->self_fd=fcntl(owner->self_fd,F_DUPFD_CLOEXEC,3);
    if(delegate->self_fd<0)goto failed;
    delegate->controller=owner->controller;
    delegate->self=owner->self;
    delegate->accepted=true;
    return 0;
failed:
    (void)framepeer_linux_close(delegate);
    return FRAMEPEER_UNAVAILABLE;
}
