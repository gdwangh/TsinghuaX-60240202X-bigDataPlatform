package com.aliyun.odps.mapred;

import java.util.ArrayList;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.conf.Configured;
import com.aliyun.odps.mapred.conf.JobConf;

public class JobRunnerImpl extends Configured implements JobRunner {
	static final String DEFAULT_WORK_DIR = "work";
	  static final String DEFAULT_MAP_OUT_DIR = "mapout";
	  
	  private JobConf jobConf;
	  private Configuration conf;
	  private LocalRunningJob rj;
	  
  @Override public RunningJob submit() throws OdpsException {
    // return null;
	    //return null;
	    
	    // get the configuration
		 conf = getConf();
		 jobConf = new JobConf(conf);
		 jobConf.set(MapperTaskContext.mapper_workDir, DEFAULT_WORK_DIR);
		 jobConf.set(MapperTaskContext.mapper_OutSubDir, DEFAULT_MAP_OUT_DIR);
				 
		 rj = new LocalRunningJob();	 
		 
		 int inst_id;
		 // mapper
		MapperTaskContext mapperContext = null;
		inst_id = 1;
		
		try {
			mapperContext = new MapperTaskContext(jobConf, inst_id);

			MapTaskRunner mr = new MapTaskRunner(mapperContext, rj);
		    Thread mt = new Thread(mr);
		    rj.add(mapperContext.getTaskID(), mt, TASK_TYPE.TASK_TYPE_MAP);

		    // rj.printTaskList();
		    
		    mt.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			TaskId taskId = null;
			if (mapperContext == null) {
				taskId = new TaskId(MapperTaskContext.StageId, inst_id);
			}  else {
				taskId = mapperContext.getTaskID();
			}
			
			rj.setStatus(taskId, LocalTaskStatus.FAILED);
			
			e.printStackTrace();

			return rj;
		}
		
		 // mergeSort
		inst_id++;
		
		TaskId mergeTaskId = new TaskId("MergeSort", inst_id);
	    MergeTaskRunner msr= new MergeTaskRunner(mergeTaskId, mapperContext,rj);
		Thread msrt = new Thread(msr);

	    rj.add(mergeTaskId, msrt, TASK_TYPE.TASK_TYPE_SORT);
	    // rj.printTaskList();

		msrt.start();
	    
		 // reducer
		 jobConf.set(MapperTaskContext.mapper_OutDir, mapperContext.getOutputDir());;
		 inst_id++;
		 try {
			ReducerTaskContext reducerContext = new ReducerTaskContext(jobConf,inst_id, false);
			
			ReduceTaskRunner rr = new ReduceTaskRunner(reducerContext,rj);
		    Thread rt = new Thread(rr);		
		    rj.add(reducerContext.getTaskID(), rt, TASK_TYPE.TASK_TYPE_REDUCE);
		    // rj.printTaskList();

		    rt.start();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		 
	     return rj;
  }
}
