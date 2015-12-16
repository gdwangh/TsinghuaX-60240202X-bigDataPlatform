#include "server.h"

using namespace std;
using namespace homework;

bool Server::UpdateTable(const string& tableName, const string& content)
{
 		/*获取table所有拷贝所在的Worker id*/
    std::vector<size_t> workers = mTables[tableName];
    
    if (workers.size() == 0)   // 全新的table，分配Work_id
    {
       if (allocWorkers() == null) {
       	   return false;
       }
       
       workers = mTables[tableName];
    }
    
    // 阶段1，参与者是否可以执行提交操作
    bool result = true;
    size_t i;
    
    for (i = 0; i < workers.size(); ++i） {
    	 result = mWorkers[workers[i]]->UpdateTable(tableName, content);
    	 if (result == false) {
    	 		break;
    	 }
    }
    
    // 阶段二，回滚 或 真正提交
    if (result == false)    // 有失败的，回滚
    {
    	 for (size_t j = 0; j < i; ++i） 
    	 {
		    	 result = mWorkers[workers[j]]->Rollback(tableName);
		    	 if (result == false) {  
		    	 	  // 回滚失败
		    	 }
		   }
		   
		   result = false;
    }  
    else // 所有都成功
    {
    	for (size_t i = 0; i < workers.size(); ++i） 
    	{
    	 		result = mWorkers[workers[i]]->Commit(tableName);
    	 		if (result == false) {  
    	 			// 提交失败, 隔离worker
    	 		}
    	 }
    	 
    	 result = true;
    }
    
    return result;
}

bool Server::allocWorkers(const string& tableName)  {
	maxCopyNum = max(mWorkers.size(), 3);
	
	// 生成[0,maxCopyNum)之间的随机整数
	 srand((unsigned)time(NULL)); 
	 size_t idx = rand() % maxCopyNum;
	 
	 // 跳到起始位置
	 std::map<uint8_t, WorkerPtr> ::iterator it = mWorkers.begin() + idx;
	 
	 std::vector<std::string> work_list;
	 	
	 // 取maxCopyNum个worker
	 for (size_t i=0; i<maxCopyNum; i++)
	 {
	 	  work_list[i] = it->first;
	 	  it++;
	 	  if (it == mWorkers.end()) 
	 	  {
	 	      it = mWorkers.begin();
	 	  }
	 	  	
	 }
	 
	 mTables[tableName] = work_list;
	 return true;
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
	  if (content.length % BlockSize > 0) fileNum++;
	  	
	  // 分块写入多个文件
	  for (size_t i=0; i< fileNum; i++) {
	  	
	  	  // 生成新文件
	  	  std::string newFileId = GetFileId();
	  	  	
	  	  if (CreateFile(newFileId) == false) 
	  	  	return false;
	  	  	
	  	  if (WriteToFile(newFileId,content.substr(i*BlockSize, BlockSize)) == false) {
	  	  	
	  	  	// 删除已经生成的临时文件
	  	  	std::for_each( tmpFileList.begin(), tmpFileList.end(), DeleteFile);
	  	  		
	  	  	return false;
	  	  }
	  	  	
	  	  tmpFileList[i] = newFileId;
	  }

    // 保存到待提交修改中
    mTableFilesTmpToCommit[tableName] = tmpFileList;
    
    return true;
	  
}


bool Worker::Commit(const std::string& tableName){
	
		std::map<std::string, std::vector<std::string> >::iterator it_new, it_old;
		
		it_new = mTableFilesTmpToCommit.find(tableName);
		it_old = mTableFiles.find(tableName);
		
		if ( it_new == mTableFilesTmp.end() && (it_old == mTableFiles.end()) {   // 没有需要提交的内容
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
		
		if ( it_new == mTableFilesTmp.end()) {   // 没有需要提交的内容
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
