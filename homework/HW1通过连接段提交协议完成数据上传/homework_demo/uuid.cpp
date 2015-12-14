#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>
#include <fstream>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include "worker.h"

using namespace std;
using namespace homework;

string Worker::GetFileId()
{
    int fd = open("/dev/urandom", O_RDONLY);
    //generate random bytes
    
    unsigned char buffer[16];
    ssize_t rbytes;
    do
    {
        rbytes = read(fd, reinterpret_cast<char*>(&buffer[0]), 16);
    }
    while ((rbytes >= 0 && rbytes < 16) || (rbytes == -1 && errno == EINTR)); 
    close(fd);

    //set version
    buffer[8] &= 0xbf;
    buffer[8] |= 0x80;

    buffer[6] &= 0x4f;
    buffer[6] |= 0x40;

    //convert random bytes into string
    static const char* format =  "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x";
    char buf[37];
    int ret;
    ret = snprintf(buf, 37, format, 
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4] ,buffer[5] ,buffer[6] ,buffer[7],
            buffer[8], buffer[9], buffer[10] ,buffer[11], buffer[12], buffer[13], buffer[14], buffer[15]);

    assert(ret == 36);
    return string(buf);
}

