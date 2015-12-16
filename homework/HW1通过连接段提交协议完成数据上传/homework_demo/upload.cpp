#include "server.h"

using namespace std;
using namespace homework;

#include <stdlib.h>
#include <algorithm>

bool Server::UpdateTable(const string& tableName, const string& content)
{
 		/* 简化问题，所有worker都参与 */
    std::map<size_t, WorkerPtr>::iterator  it_workers;
    std::vector<size_t> worker_list;
    	    
    // 阶段1，参与者是否可以执行提交操作
    bool result = true;
    
    for (it_workers = mWorkers.begin(); it_workers != mWorkers.end(); ++it_workers) {
    	 result = it_workers->second->UpdateTable(tableName, content);

    	 if (result == false) {
    	 		cout<<"worker "<<it_workers->first<<" UpdateTable fail"<<endl;
    	 		
    	 		break;
    	 }
    }
        
    // 阶段二，回滚 或 真正提交
    if (result == false)    // 有失败的，回滚
    {
    	 for (it_workers = mWorkers.begin(); it_workers != mWorkers.end(); ++it_workers)
    	 {
		    	 result = it_workers->second->Rollback(tableName);
		    	 if (result == false) {  
		    	 	  // 回滚失败
		    	 }
		   }
		   
		   result = false;
    }  
    else // 所有都成功
    {
    	for (it_workers = mWorkers.begin(); it_workers != mWorkers.end(); ++it_workers)
    	{
    	 		result = it_workers->second->Commit(tableName);
    	 		
    	 		if (result == false) {  
    	 			// 提交失败, 隔离worker
    	 		}  else {
    	 			worker_list.push_back(it_workers->first);
    	 		}
    	 }
    	 
    	 result = true;
    }
    
    mTables[tableName] = worker_list;
        
    return result;
}


bool Worker::UpdateTable(const string& tableName, const string& content)
{	 
	  std::map<std::string, std::vector<std::string> >::iterator it;
	  std::vector<std::string> tmpFileList;
	  	
	  	
	  if ( (it = mTableFilesTmpToCommit.find(tableName) ) != mTableFilesTmpToCommit.end()) { // 有未提交或回滚的数据
	  	return false;
	  } 	 
	  
	  // 计算要分多少块
	  int fileNum = content.length()/BlockSize;
	  if (content.length() % BlockSize > 0) fileNum++;
	  	
	  // 分块写入多个文件
	  for (size_t i=0; i< fileNum; i++) {
	  	
	  	  // 生成新文件
	  	  std::string newFileId = GetFileId();
	  	  	
	  	  if (CreateFile(newFileId) == false) 
	  	  	return false;
	  	  	
	  	  if (WriteToFile(newFileId,content.substr(i*BlockSize, BlockSize)) == false) {
	  	  	
	  	  	// 删除已经生成的临时文件
			for (size_t j=0; j<tmpFileList.size();j++) 
			{
				   DeleteFile(tmpFileList[j]);
			}
	  	  		
	  	  	return false;
	  	  }
	  	  	
	  	  tmpFileList.push_back(newFileId);
	  }

    // 保存到待提交修改中
    mTableFilesTmpToCommit[tableName] = tmpFileList;
    
    return true;
	  
}


bool Worker::Commit(const std::string& tableName){
	
		std::map<std::string, std::vector<std::string> >::iterator it_new, it_old;
		
		it_new = mTableFilesTmpToCommit.find(tableName);
		it_old = mTableFiles.find(tableName);
		
		if ( (it_new == mTableFilesTmpToCommit.end()) && (it_old == mTableFiles.end())) {   // 没有需要提交的内容
	  	return true;
	  }
	  
	  std::vector<std::string> files;
	  if ( it_old != mTableFiles.end()) {   // 待删除的旧文件
	  		files = it_old->second;
	  }
	  
	  if (it_new != mTableFilesTmpToCommit.end() ) {  // 待加入的新文件
	  		mTableFiles[tableName] = it_new->second;  
	  	  mTableFilesTmpToCommit.erase(it_new);
	  }
	  
	  // 删除旧文件		
	  for (size_t i=0; i<files.size(); i++) {
		 	  DeleteFile(files[i]);
		}
		return true;
	  
}

bool Worker::Rollback(const std::string& tableName){
	
		std::map<std::string, std::vector<std::string> >::iterator it_new;
		it_new = mTableFilesTmpToCommit.find(tableName);

		if ( it_new == mTableFilesTmpToCommit.end()) {   // 没有需要提交的内容
	  	return true;
	  }
	  
	  // 删除新文件
	  std::vector<std::string> files = it_new->second;
	  mTableFilesTmpToCommit.erase(it_new);

	  for (size_t i=0; i<files.size(); i++) {
		 	  DeleteFile(files[i]);
		}
		
	  return true;
}
