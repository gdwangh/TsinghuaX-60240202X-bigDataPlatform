#ifndef TEST_H
#define TEST_H

#include "test_server.h"

namespace homework
{

class Test
{
public:
    static void TestUploadNegetive();
private:
    static bool TestUploadInternal(TestServer* testServer);
};
    
}

#endif
