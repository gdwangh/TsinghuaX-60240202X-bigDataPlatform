#ifndef WORKER_H
#define WORKER_H

#include <iostream>
#include <map>
#include <vector>

namespace homework
{
class Worker
{
public:
    Worker(uint8_t workerId)
    : mWorkerId(workerId)
    {}

    /*需要实现的函数。把content的内容写到tableName所指定的副本中*/
    bool UpdateTable(const std::string& tableName, const std::string& content);
    	
    bool Commit(const std::string& tableName);

    bool Rollback(const std::string& tableName);


    /*读取副本的内容*/
    bool ReadTable(const std::string& tableName, std::string& content);
    	

private:
	  /*可以获取一个全局唯一的文件id*/
    static std::string GetFileId();
    
    /*Worker对文件的操作，如果返回true说明操作成功，返回false说明执行失败*/
    bool CreateFile(const std::string& fileId);

    bool DeleteFile(const std::string& fileId);

    bool RenameFile(const std::string& oriFileId, const std::string& newFileId);

    bool WriteToFile(const std::string& fileId, const std::string& content);

    bool ReadFile(const std::string& fileId, std::string& content);    

    /*worker的id*/
    uint8_t mWorkerId;
    
    /*表名对其存储文件list的映射*/
    std::map<std::string, std::vector<std::string> > mTableFiles;
    	
    std::map<std::string, std::vector<std::string> > mTableFilesTmpToCommit;   /* 第一阶段已经删除，单还没有commit或rollback的 */
    	
    /*文件id与文件真实路径的映射*/
    std::map<std::string, std::string> mFiles;
    	
    /* BlockSize */
    static const int BlockSize = 1024;
};
typedef Worker* WorkerPtr;

}
#endif
