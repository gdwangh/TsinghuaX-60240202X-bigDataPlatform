package com.aliyun.odps.mapred;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.volume.FileSystem;

public class ReducerTaskContext implements TaskContext {
	private final int MAX_REDUCER_FILE_RECNUM = 1000000;

	private JobConf jobConf;
	private Counters reducerCounters;
	private TaskId taskId;
	
	private final String StageId = "REDUCE";

	private LocalRecordReader reader;
	
	private String inputDir;	
	private ArrayList<String> inputFileList;
	private int fileId;  // input
	
	private List<Record> curValuesList;
	
	private LocalRecordWriter writer;
	
	private boolean splitOutFile;
	private int file_cnt;
	private String outputFileRoot;
	
	ReducerTaskContext(JobConf job, int inst_id, boolean splitOutFlag) throws IOException {
		this.jobConf = job;
		this.reducerCounters = new Counters();
		this.taskId = new TaskId(StageId, inst_id);
			
		inputFileList = null;
		
		// output 
		this.splitOutFile = splitOutFlag;
		file_cnt = 0;
		outputFileRoot = this.getOutputTableInfo()[0].toString();
		
		String outputFileName=null;
		if (splitOutFile) {
			outputFileName = outputFileRoot + "."+file_cnt;
		}  else {
			outputFileName = outputFileRoot;
		}

		// System.out.println(outputFileName);
		writer = new LocalRecordWriter(outputFileName);
		
	}
	
	@Override
	public Record createMapOutputKeyRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getMapOutputKeySchema());
	}

	@Override
	public Record createMapOutputValueRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getMapOutputValueSchema());
	}

	@Override
	public Record createOutputKeyRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getMapOutputKeySchema());
	}

	@Override
	public Record createOutputRecord() throws IOException {
		// TODO Auto-generated method stub
		LocalRecord key = new LocalRecord(jobConf.getMapOutputKeySchema());
		key.addColumns(getMapOutputValueSchema());
		
		return key;
	}

	@Override
	public Record createOutputRecord(String label) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record createOutputValueRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getMapOutputValueSchema());
	}

	@Override
	public Counter getCounter(Enum<?> counterName) {
		// TODO Auto-generated method stub
		return reducerCounters.findCounter(counterName);
	}

	@Override
	public Counter getCounter(String group, String name) {
		// TODO Auto-generated method stub
		return reducerCounters.findCounter(group, name);
	}

	@Override
	public FileSystem getInputVolumeFileSystem() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileSystem getInputVolumeFileSystem(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VolumeInfo getInputVolumeInfo() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VolumeInfo getInputVolumeInfo(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableInfo[] getOutputTableInfo() throws IOException {
		// TODO Auto-generated method stub
		return OutputUtils.getTables(jobConf);
	}

	@Override
	public FileSystem getOutputVolumeFileSystem() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileSystem getOutputVolumeFileSystem(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VolumeInfo getOutputVolumeInfo() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VolumeInfo getOutputVolumeInfo(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskId getTaskID() {
		// TODO Auto-generated method stub
		return this.taskId;
	}

	@Override
	public void progress() {
		// TODO Auto-generated method stub

	}

	@Override
	public BufferedInputStream readResourceFileAsStream(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Record> readResourceTable(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(Record record) throws IOException {
		// TODO Auto-generated method stub
		if ((splitOutFile) && (writer.getWrittenRecCount() == MAX_REDUCER_FILE_RECNUM)) {
			closeOutput();

			file_cnt++;
			String outputFileName = outputFileRoot + "."+file_cnt;
			//System.out.println("******* "+outputFileName);
			writer = new LocalRecordWriter(outputFileName);
		}
		
		//printRecord(record);
		writer.write(record);
		writer.endLine();
	}

	@Override
	public void write(Record record, String label) throws IOException {
		// TODO Auto-generated method stub
	
	}

	@Override
	public void write(Record key, Record value) throws IOException {
		// TODO Auto-generated method stub		
	}

	@Override
	public Class<? extends Reducer> getCombinerClass() throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return jobConf.getCombinerClass();
	}

	@Override
	public String[] getGroupingColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobConf getJobConf() {
		// TODO Auto-generated method stub
		return jobConf;
	}

	@Override
	public Column[] getMapOutputKeySchema() {
		// TODO Auto-generated method stub
		return jobConf.getMapOutputKeySchema();
	}

	@Override
	public Column[] getMapOutputValueSchema() {
		// TODO Auto-generated method stub
		return jobConf.getMapOutputValueSchema();
	}

	@Override
	public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return jobConf.getMapperClass();
	}

	@Override
	public int getNumReduceTasks() {
		// TODO Auto-generated method stub
		return jobConf.getNumReduceTasks();
	}

	@Override
	public Class<? extends Reducer> getReducerClass() throws ClassNotFoundException {
		// TODO Auto-generated method stub
		return jobConf.getReducerClass();
	}

	@Override
	public Record getCurrentKey() {
		// TODO Auto-generated method stub
		Record curKey = reader.getCurrentRecord(jobConf.getMapOutputKeySchema(), 0);
		
		return curKey;
	}

	@Override
	public Iterator<Record> getValues() {
		// TODO Auto-generated method stub
		curValuesList = new ArrayList<Record>();
		StringRecordComparator cmptor = new StringRecordComparator();
		
		Record curKey = getCurrentKey();
		Boolean hasNextkey = true;
		Boolean isSameKey = true;
		while ( hasNextkey && isSameKey) {
			Record curValue = getCurrentValue();
			curValuesList.add(curValue);
			
			hasNextkey = nextKeyValue();
			if (! this.isInputClose()) {
				isSameKey = (cmptor.compare(getCurrentKey(), curKey)==0);
			}
		};
		
		return curValuesList.iterator();
	}

	@Override
	public boolean nextKeyValue() {
		// TODO Auto-generated method stub		
		
		try {
			if (inputFileList == null) {
				inputFileList = new ArrayList<String>();
				inputDir = jobConf.get(MapperTaskContext.mapper_OutDir);
				Path inputPath = Paths.get(inputDir);
				for (Path f :  Files.newDirectoryStream(inputPath)) {
					if (Files.isDirectory(f))
							continue;
					
					// file
					inputFileList.add(f.toString());
				}
				fileId = 0;
				reader = new LocalRecordReader(inputFileList.get(fileId));
			}
			
			if (isInputClose()) return false;
			
			Boolean hasNext = reader.hasNextRecord();
			if (!hasNext) {
				reader.close();
				fileId++;
				if (fileId < inputFileList.size())  {  // there are input files to be handled
					reader = new LocalRecordReader(inputFileList.get(fileId));
					hasNext = reader.hasNextRecord();
				}
			}
			return hasNext;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	public Record getCurrentValue() {
		int keyColumnNum = jobConf.getMapOutputKeySchema().length;
		Record curValue = reader.getCurrentRecord(jobConf.getMapOutputValueSchema(),keyColumnNum);
		
		return curValue;
	}

	public void printInputFileList() {
		for (String fn : inputFileList) {
			System.out.println("input file: " + fn);
		}
	}
	
	public Boolean isInputClose() {
		return (fileId >= inputFileList.size());
	}
	
	public void closeOutput() throws IOException {
		writer.close();
	}

}
