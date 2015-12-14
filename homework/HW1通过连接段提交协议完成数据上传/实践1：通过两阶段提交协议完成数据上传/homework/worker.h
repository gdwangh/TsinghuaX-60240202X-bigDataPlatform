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

    bool UpdateTable(const std::string& tableName, const std::string& content);

    bool ReadTable(const std::string& tableName, std::string& content);

private:
    static std::string GetFileId();
    
    bool CreateFile(const std::string& fileId);

    bool DeleteFile(const std::string& fileId);

    bool RenameFile(const std::string& oriFileId, const std::string& newFileId);

    bool WriteToFile(const std::string& fileId, const std::string& content);

    bool ReadFile(const std::string& fileId, std::string& content);    

    uint8_t mWorkerId;
    std::map<std::string, std::vector<std::string> > mTableFiles;
    std::map<std::string, std::string> mFiles;
};
typedef Worker* WorkerPtr;

}
#endif
