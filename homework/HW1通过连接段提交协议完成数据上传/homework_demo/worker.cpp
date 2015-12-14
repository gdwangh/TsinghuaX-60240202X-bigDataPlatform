#include "worker.h"
#include <fstream>
#include <cstdio>
#include <sys/stat.h>
#include <sys/types.h>

using namespace homework;
using namespace std;

string ToString(size_t num)
{
    char buf[20];
    snprintf(buf, sizeof(buf), "%u", num);
    return string(buf);
}

bool Worker::CreateFile(const string& fileId)
{
    if (mFiles.find(fileId) != mFiles.end())
    {
        cerr << "file " << fileId << " already exists from worker " << ToString(mWorkerId) << " when create file" << endl;
        return false;
    }
    
    mkdir(ToString(mWorkerId).c_str(), S_IRWXU);
    string fileName = ToString(mWorkerId) + "/" + fileId;
    ofstream ofs;
    ofs.open(fileName.c_str(), ofstream::out | ofstream::trunc);
    ofs.close();
    mFiles[fileId] = fileName;
    return true;
}

bool Worker::DeleteFile(const string& fileId)
{
    map<string, string>::iterator it;
    if ((it = mFiles.find(fileId)) == mFiles.end())
    {
        cerr << "Can not find file id " << fileId << " from worker " << ToString(mWorkerId) << " when delete file" << endl;;
        return false;
    }

    remove(it->second.c_str());
    mFiles.erase(it);
    return true;
}

bool Worker::RenameFile(const string& oriFileId, const string& newFileId)
{
    map<string, string>::iterator it;
    if ((it = mFiles.find(oriFileId)) == mFiles.end())
    {
        cerr << "Can not find file id " << oriFileId << " from worker " << ToString(mWorkerId) << " when rename file" << endl;
        return false;
    }

    if (mFiles.find(newFileId) != mFiles.end())
    {
        cerr << "file id " << newFileId << " already exists from worker " << ToString(mWorkerId) << " when rename file" << endl;
        return false;
    }
    
    mFiles[newFileId] = it->second;
    mFiles.erase(it);
    return true;
}

bool Worker::WriteToFile(const string& fileId, const string& content)
{
    map<string, string>::iterator it;
    if ((it = mFiles.find(fileId)) == mFiles.end())
    {
        cerr << "Can not find file id " << fileId << " from worker " << ToString(mWorkerId) << " when write file" << endl;
        return false;
    }

    string fileName = it->second;
    ofstream ofs;
    ofs.open(fileName.c_str(), ofstream::out | ofstream::trunc);
    ofs.write(content.c_str(), content.size());
    ofs.close();
    return true;
}

bool Worker::ReadFile(const string& fileId, string& content)
{
    map<string, string>::iterator it;
    if ((it = mFiles.find(fileId)) == mFiles.end())
    {
        cerr << "Can not find file id " << fileId << " from worker " << ToString(mWorkerId) << " when read file" << endl;
        return false;
    }

    string fileName = it->second;
    ifstream ifs;
    ifs.open(fileName.c_str(), ofstream::in);
    char buf[50];
    while (ifs.good())
    {
        ifs.read(buf, 50);
        content.append(buf, ifs.gcount());
    }
    ifs.close();
    return true;
}

bool Worker::ReadTable(const string& tableName, string& content)
{
    map<string, vector<string> >::iterator it;
    if ((it = mTableFiles.find(tableName)) == mTableFiles.end())
    {
        cerr << "Can not find table name " << tableName << " from worker " << ToString(mWorkerId) << " when read table" << endl;
        return false;
    }

    content.clear();
    vector<string> files = it->second;
    for (size_t i = 0; i < files.size(); ++i)
    {
        string result;
        if (!ReadFile(files[i], result))
        {
            content.clear();
            return false;
        }
        else
        {
            content.append(result);
        }
    }
    return true;
}
