package com.aliyun.odps.mapred;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;

public class MergeTaskRunner implements Runnable {
	private MapperTaskContext context;
	private TASK_TYPE taskType;
	private LocalRunningJob runningInfo;
	private TaskId taskId;
	private Collection<Thread> waitList;
	
	MergeTaskRunner(TaskId taskId, MapperTaskContext context, LocalRunningJob rj) {
		this.context = context;
		this.taskType = TASK_TYPE.TASK_TYPE_SORT;
		this.waitList = rj.getMapThreadList();
		
		this.taskId = taskId;
		this.runningInfo = rj;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("merge sort waiting for map.");

		 try {
				runningInfo.setStatus(taskId, LocalTaskStatus.WAITNG);
				for (Thread mt : waitList) {
					mt.join();
				}
				
		  } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);

				return;
		  }
		
		System.out.println("Running merge sort.");
		try {
			if (runningInfo.getMapStatus() != LocalTaskStatus.SUCCEEDED)
			{
				runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);
				
				System.out.println("merge sort is killed.");
				// runningInfo.printTaskList();
				
				return;
			}
			
			runningInfo.setStatus(taskId, LocalTaskStatus.RUNNING);
			mergeSortExecute();
			runningInfo.setStatus(taskId, LocalTaskStatus.SUCCEEDED);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);

		}
		
		// test
		// runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
		// runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);
	}
	
	private void mergeSortExecute() throws IOException, InterruptedException {
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		
		List<String> mapOutputList = context.getMapOutputFiles();  
		String mergeFileName = context.getOutputDir()+context.getTaskID();
		
		//System.out.println("Merge sorted file");
		mergeSortFiles(mapOutputList, mergeFileName);
		
		 
	}
	
	public void mergeSortFiles(List<String> files, String outFileName) throws IOException, InterruptedException {	  	  
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		
		if (files.size()==1)  {
			// 只有一个文件，直接改名
			  Path source = Paths.get(files.get(0));	  
			  Path target = Paths.get(outFileName);
			  Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
			 
			  return;
		  }
		
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		
		 // open files
		 ArrayList<LocalRecordReader> readerList = new ArrayList<LocalRecordReader>();
		 int filecnt = 0;
		  
		 for (Iterator<String> it = files.iterator();it.hasNext();) {
			  LocalRecordReader reader = new LocalRecordReader(it.next());
			  if (reader.hasNextRecord()) {
				  readerList.add(reader);
				  filecnt++;
			  }
			  
			  if (Thread.currentThread().isInterrupted()) break;
		  }

		 if (Thread.currentThread().isInterrupted()) {
			 for (LocalRecordReader reader : readerList) {
				 reader.close();
			 }
			 throw new InterruptedException();
		 }
		 
		  LocalRecordWriter writer = new LocalRecordWriter(outFileName);
		  
		  StringRecordComparator comparator = new StringRecordComparator();
		  
		  while ((!Thread.currentThread().isInterrupted()) && (filecnt>1)) {
			  
			  Record minRec = null;
			  int fileId = 0;
			  
			  for (int i=0; i<readerList.size(); i++) {
				  Record curRec = readerList.get(i).read();
				  
				  if (minRec == null) {
					  minRec = curRec;
					  fileId = i;
					  continue;
				  }
				  
				  if (comparator.compare(minRec, curRec) > 0) {
					  minRec = curRec;
					  fileId = i;
				  }
			  }
			  
			  writer.write(minRec);
			  writer.endLine();
			  
			  if (!readerList.get(fileId).hasNextRecord()) {
				  readerList.get(fileId).close();
				  readerList.remove(fileId);
				  filecnt--;
			  }

		  }  
		  
		  if (Thread.currentThread().isInterrupted()) {
				 for (LocalRecordReader reader : readerList) {
					 reader.close();
				 }
				 writer.close();
				 writer.delete();
				 throw new InterruptedException();
			 }
		  
		  // 剩下的一次性写入输出中
		  LocalRecordReader reader = readerList.get(0);
		  do  {
			  writer.write(reader.read());
			  writer.endLine();
		  } while (reader.hasNextRecord());
		  
		  reader.close();
		  writer.close();
		  
		  // 删除输入文件
		  for (String file : files) {
			  Path p = Paths.get(file);
			  Files.delete(p);
		  }
	  }

	public TASK_TYPE getTaskType() {
		return this.taskType;
	}
}
