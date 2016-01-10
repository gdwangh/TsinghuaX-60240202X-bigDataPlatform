package com.aliyun.odps.mapred;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.csvreader.CsvWriter;

public class LocalRecordWriter implements RecordWriter {
	private CsvWriter writer;
	private long writeNum;
	private String fileName;
	
	public LocalRecordWriter(String fileName) throws IOException {	
		Path outFile = Paths.get(fileName);
		// System.out.println(outFile.toString() + ": "+outFile.getParent());
		
		if ((outFile.getParent() != null) && (Files.notExists(outFile.getParent()))) {
			Files.createDirectories(outFile.getParent());
		}		
		
		writer = new CsvWriter(fileName);
		writeNum = 0;
		this.fileName = new String(fileName);
		//System.out.println("write in "+fileName);
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		writer.flush();
		writer.close();
	}

	@Override
	public void write(Record rec) throws IOException {
		// TODO Auto-generated method stub
		for (Object cv: rec.toArray()) {
			writer.write(cv.toString());
		}
	}
	
	public void endLine() throws IOException {
		writer.endRecord();
		writeNum++;
	}

	public long getWrittenRecCount() {
		return writeNum;
	}
	
	public void delete() throws IOException {
		Path outfile = Paths.get(fileName);
		Files.deleteIfExists(outfile);
	}
}
