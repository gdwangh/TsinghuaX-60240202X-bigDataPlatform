#ifndef SERVER_H
#define SERVER_H

#include <iostream>
#include <map>
#include <vector>
#include <stdint.h>

#include "worker.h"

namespace homework
{

class Server
{
public:
    Server();

    ~Server();
    
    /*需要实现的函数。把content的内容写到tableName指定的表中*/

    bool UpdateTable(const std::string& content, const std::string& tableName);
    
private:
	  // workerId 与 指向该worker的指针
    std::map<uint8_t, WorkerPtr> mWorkers;
    	
    // Table名和其拷贝所在Worker的 list 
    /* table名与table拷贝所存放worker的映射，由于一个table可能有多份拷贝，所以可能存放在多个Worker上*/
    std::map<std::string, std::vector<uint8_t> > mTables;
    	 
    
};
}

#endif
