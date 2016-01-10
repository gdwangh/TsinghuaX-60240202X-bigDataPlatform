package com.aliyun.odps.mapred;

import java.io.IOException;
import java.util.Collection;

public class ReduceTaskRunner implements Runnable {
	private ReducerTaskContext context;
	private TASK_TYPE taskType;
	private LocalRunningJob runningInfo;
	private TaskId taskId;
	private Collection<Thread> waitList;

	ReduceTaskRunner(ReducerTaskContext context, LocalRunningJob rj) {
		this.context = context;
		this.taskType = TASK_TYPE.TASK_TYPE_REDUCE;
		
		this.taskId = context.getTaskID();
		this.runningInfo = rj;
		
		this.waitList = rj.getReduceWaitingThreadList();

	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		runningInfo.setStatus(taskId, LocalTaskStatus.WAITNG);

		try {
			for (Thread mt : waitList) {
				mt.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			runningInfo.setStatus(taskId, LocalTaskStatus.KILLED);
			return;
		}
		
		System.out.println("Running reduce.");
		try {
			if (runningInfo.getJobStatus() == JobStatus.FAILED)
			{
					runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);
					return;
			}
			
			runningInfo.setStatus(taskId, LocalTaskStatus.RUNNING);
			
			reducerTaskExecute();
			
			runningInfo.setStatus(taskId, LocalTaskStatus.SUCCEEDED);

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);

		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);

		} catch (ClassNotFoundException e) {
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
			runningInfo.setStatus(taskId, LocalTaskStatus.FAILED);

		}
	}
	
	private void reducerTaskExecute() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, InterruptedException {
		Reducer reducer = (Reducer)context.getReducerClass().newInstance();
		reducer.setup(context);
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		}
		
		// 判断下一条记录是否是同一个key的时候，getValues()实际会多读一行记录
		Boolean isPreRead = false;
		
		while ((!Thread.currentThread().isInterrupted()) && ((isPreRead) || context.nextKeyValue())) {			
			// printRecord(reducerContext.getCurrentKey(), reducerContext.getValues()); 
			reducer.reduce(context.getCurrentKey(), context.getValues(), context);

			isPreRead = !context.isInputClose();
		}
		
		context.closeOutput();
		
		if (Thread.currentThread().isInterrupted()) {
			 throw new InterruptedException();
		}
	}

	public TASK_TYPE getTaskType() {
		return this.taskType;
	}
}
