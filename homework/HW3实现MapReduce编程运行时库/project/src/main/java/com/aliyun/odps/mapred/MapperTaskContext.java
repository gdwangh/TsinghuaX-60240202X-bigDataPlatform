package com.aliyun.odps.mapred;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.volume.FileSystem;


public class MapperTaskContext implements Mapper.TaskContext {		
	static final String mapper_workDir = "default.work";
	static final String mapper_OutSubDir = "default.map.output.subdir";
	static final String mapper_OutDir = "default.map.output.dir";

	static final String StageId = "MAP";
	private final long MAX_MAPPER_FILE_RECNUM = 1000000;
	
	private String Separator;
	
	private JobConf jobConf;
	private Counters mapCounters;
	private TaskId taskId;
	
	private LocalRecordReader reader;
	
	private LocalRecordWriter writer;
	private List<String> outputFiles;
	private int file_cnt;
	private String outputDir;
	
	MapperTaskContext(JobConf job, int inst_id) throws Exception {
		this.jobConf = job;
		this.mapCounters = new Counters();
		this.taskId = new TaskId(StageId, inst_id);

		reader = new LocalRecordReader(this.getInputTableInfo().toString());
		
		Separator = FileSystems.getDefault().getSeparator();
		
		file_cnt = 0;
		outputDir = jobConf.get(mapper_workDir) + Separator
					+jobConf.get(mapper_OutSubDir) + Separator+ taskId + Separator;
		String outputFileName = outputDir + Separator
								+this.taskId+"_"+file_cnt+".cvs";
		
		outputFiles = new ArrayList<String>();
		outputFiles.add(outputFileName);
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

	@SuppressWarnings("deprecation")
	@Override
	public Record createOutputRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getOutputSchema());
	}

	@SuppressWarnings("deprecation")
	@Override
	public Record createOutputRecord(String label) throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getOutputSchema(label));
	}

	@Override
	public Record createOutputValueRecord() throws IOException {
		// TODO Auto-generated method stub
		return new LocalRecord(jobConf.getMapOutputValueSchema());
	}

	@Override
	public Counter getCounter(Enum<?> name) {
		// TODO Auto-generated method stub
		return mapCounters.findCounter(name);
	}

	@Override
	public Counter getCounter(String group, String name) {
		// TODO Auto-generated method stub
		return mapCounters.findCounter(group, name);
	}

	@Override
	public FileSystem getInputVolumeFileSystem(String uri) throws IOException {
		// TODO Auto-generated method stub
		// String separator = FileSystems.getDefault().getSeparator();
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
		return taskId;
	}

	@Override
	public void progress() {
		// TODO Auto-generated method stub
	}

	@Override
	public BufferedInputStream readResourceFileAsStream(String resourceName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Record> readResourceTable(String resourceName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(Record record) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(Record record, String label) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(Record key, Record value) throws IOException {
		// TODO Auto-generated method stub
	
		if (writer.getWrittenRecCount() == MAX_MAPPER_FILE_RECNUM) {
			closeOutput();

			file_cnt++;
			String outputFileName = outputDir + Separator
									+this.taskId+"_"+file_cnt+".cvs";

			outputFiles.add(outputFileName);
			writer = new LocalRecordWriter(outputFileName);
		}
		
		writer.write(key);
		writer.write(value);
		writer.endLine();
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
	public Record getCurrentRecord() {		
		return reader.read();
	}

	@Override
	public long getCurrentRecordNum() {
		// TODO Auto-generated method stub
		return reader.getCurrentRecordNum();
	}

	@Override
	public TableInfo getInputTableInfo() {
		// TODO Auto-generated method stub
		return InputUtils.getTables(jobConf)[0];
	}

	@Override
	public boolean nextRecord() {
		// TODO Auto-generated method stub
		try {
			return reader.hasNextRecord();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		return false;
	}
	
	@Override
	public FileSystem getInputVolumeFileSystem() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void closeOutput() throws IOException {
		writer.close();
		//System.out.println("close output file");
	}
	
	public void closeInput() throws IOException {
		reader.close();
	}
	
	public List<String> getMapOutputFiles() {
		return outputFiles;
	}
	
	public String getOutputDir() {
		return outputDir;
	}
}
