#ifndef MOCK_SERVER_H
#define MOCK_SERVER_H

#include "test_worker.h"
#include "server.h"

namespace homework
{

class TestServer : public Server
{
public:
    TestServer()
    {
        for (size_t i = 0; i < 3; ++i)
        {
            mWorkers[i] = new Worker(i);
        }
    }
    virtual ~TestServer()
    {
        for (std::map<size_t, WorkerPtr>::iterator it = mWorkers.begin(); it != mWorkers.end(); ++it)
        {
            delete it->second;
        }
    }

    void InitTableForTest(const std::string& tableName, const std::string& content)
    {
        UpdateTable(tableName, content);
        mWorkers[3] = new TestWorker(3);
    }
};

}

#endif
