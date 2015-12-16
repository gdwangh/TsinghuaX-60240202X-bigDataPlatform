#include "test.h"
#include "test_server.h"
#include <string>
#include <vector>

using namespace homework;
using namespace std;

void Test::TestUploadNegetive()
{
    TestServer* demo = new TestServer();
    if(TestUploadInternal(demo))
    {
        cout << "demo pass" << endl;
    }
    else
    {
        cout << "demo fail" << endl;
    }
}

bool Test::TestUploadInternal(TestServer* testServer)
{
    std::string tableName = "demo";
    std::string content = "this is a demo string";

    /*在init过程会调用正常的UpdateTable逻辑*/
    testServer->InitTableForTest(tableName, content);

    cout<<"have "<<testServer->mWorkers.size()<<" workers"<<endl;
    /*获取table所有拷贝所在的Worker id*/
    cout<<"case 1"<<endl;
    std::vector<size_t> workers = testServer->mTables["demo"];

    if (workers.size() == 0)
    {
	cout<<"case 1 fail."<<endl;
        return false;
    }
    cout<<"have "<<workers.size()<<" workers"<<endl;

    /*检查每一个拷贝是否都写成功*/
    cout<<"case 2"<<endl;
    for (size_t i = 0; i < workers.size(); ++i)
    {
        std::string result;
        testServer->mWorkers[workers[i]]->ReadTable(tableName, result);
        if (result != content)
        {
	    cout<<"case 2 fail."<<endl;
            return false;
        }
    }

    /*添加了test worker后，update会失败*/
    /* InitTableForTest() 中，添加了一个test_worker，它createFile时永远返回false，因此update也应该返回false */
    /* 这里是测试党有一个work失败时，是否能够正常处理 */
    cout<<"case 3"<<endl;
    if (testServer->UpdateTable(tableName, content) != false)
    {
	cout<<"case 3 fail!"<<endl;
        return false;
    }

    /*检查之前写入的表是否还能被成功读出*/
    cout<<"case 4"<<endl;
    std::vector<size_t> newWorkers = testServer->mTables["demo"];
    for (size_t i = 0; i < newWorkers.size(); ++i)
    {
        std::string result;
        testServer->mWorkers[newWorkers[i]]->ReadTable(tableName, result);
        if (result != content)
        {
	    cout<<"case 4 fail!"<<endl;
            return false;
        }
    }
    return true;    
}
