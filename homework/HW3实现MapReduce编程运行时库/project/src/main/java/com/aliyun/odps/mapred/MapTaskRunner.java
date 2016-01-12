package com.aliyun.odps.mapred;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MapTaskRunner implements Runnable {

	private MapperTaskContext context;
	private TASK_TYPE taskType;
	private LocalRunningJob runningInfo;
	private TaskId taskId;
	
	public MapTaskRunner(MapperTaskContext context, LocalRunningJob rj) throws Exception {
		this.taskType = TASK_TYPE.TASK_TYPE_MAP;
		this.context = context;
		this.taskId = context.getTaskID();
		this.runningInfo = rj;
	}
	
	private void mapTaskExecute() throws IllegalAccessException, ClassNotFoundException, InstantiationException, IOException, InterruptedException {
		 if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		 
		 Mapper mapper = (Mapper)context.getMapperClass().newInstance();
		 
		 if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		 
		 mapper.setup(context);
		 
		 if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		 
		 while ((!Thread.currentThread().isInterrupted()) && (context.nextRecord()) ){
			mapper.map(context.getCurrentRecordNum(), context.getCurrentRecord(),context);
		 } 
			
		 context.closeOutput();
		 context.closeInput();
		 
		 if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
	}
	
	private void sortFileExecute() throws InterruptedException {
		List<String> mapOutputList = context.getMapOutputFiles();
		try {
			  for (String fn : mapOutputList) {
				  //System.out.println("Sort "+fn);
				  if (Thread.currentThread().isInterrupted()) {
						 throw new InterruptedException();
					 }
				  
				  fileSort(fn, context);
			  }
			  
		  } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
	}
	
	private void fileSort(String fileName, MapperTaskContext context) throws IOException, InterruptedException {  
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		 }
		
		List<LocalRecord> recList = new ArrayList<LocalRecord>();
		  
		  // read into memory
		LocalRecordReader reader = new LocalRecordReader(fileName);
		  
		while ((!Thread.currentThread().isInterrupted()) && (reader.hasNextRecord())) {
			LocalRecord rec = (LocalRecord)reader.read();
			
			recList.add(new LocalRecord(rec));
		  }
		  
		  reader.close();
		  
		  if (Thread.currentThread().isInterrupted()) {
				 throw new InterruptedException();
			}
		  
		  // sort
		  StringRecordComparator comparator = new StringRecordComparator();
		  Collections.sort(recList, comparator);
		  
		  if (Thread.currentThread().isInterrupted()) {
				 throw new InterruptedException();
			 }
		  
		  // write 
		  LocalRecordWriter writer = new LocalRecordWriter(fileName+".sorted");
		  for (int i=0; i<recList.size(); i++) {
			  writer.write(recList.get(i));
			  writer.endLine();
		  }
		  writer.close();
		  
		  Path infn = Paths.get(fileName);	  
		  Path sortedfn = Paths.get(fileName+".sorted");
		  try {
			  Files.move(sortedfn, infn, StandardCopyOption.REPLACE_EXISTING);
		  } catch (Exception e) {
			  e.printStackTrace();
		  } 

	  } 
	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Running mapper");
		try {
			runningInfo.setStatus(taskId, LocalTaskStatus.RUNNING);
			mapTaskExecute();
			
			if (Thread.currentThread().isInterrupted()) {
				runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);
				return;
			 }
			Thread.sleep(2000);
			sortFileExecute();

			runningInfo.setStatus(taskId, LocalTaskStatus.SUCCEEDED);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
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

	public TASK_TYPE getTaskType() {
		return this.taskType;
	}
}
