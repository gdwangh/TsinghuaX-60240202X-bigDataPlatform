package com.aliyun.odps.mapred;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.csvreader.CsvReader;

public class LocalRecordReader implements RecordReader {
	private CsvReader reader;
	private LocalRecord curRecord;
	private Column[] schema;
	
	public LocalRecordReader(String fileName) throws IOException {
		
		reader = new CsvReader(fileName);

		reader.readHeaders();
		String[] headers = reader.getHeaders();
		
		curRecord = new LocalRecord();	
		for (int i=0; i<headers.length; i++) {
			curRecord.addColumn("Col"+i, OdpsType.STRING);
		}	
		
		schema = curRecord.getColumns();
		
		// reopen the file to reset to the begin rec
		reader.close();
		reader = new CsvReader(fileName);

	}

		
	public int getColumnCount() {
		return reader.getColumnCount();
	}
	
	
	@Override
	public Record read() {  // 读取当前记录
		// TODO Auto-generated method stub
		return getCurrentRecord(schema, 0);
	}
	
	public long getCurrentRecordNum() {
		// TODO Auto-generated method stub
		return reader.getCurrentRecord();
	}
	
	public Record getCurrentRecord(Column[] outputSchema, int begin_pos) {
		// TODO Auto-generated method stub
		if (outputSchema == null) {
			outputSchema = schema;
		}
		
		Record rec = new LocalRecord(outputSchema);
		
		try {
			String[] readRec = reader.getValues();

			for (int idx = 0; (idx<outputSchema.length) && (idx < schema.length - begin_pos); idx++ ) {
				String value = readRec[begin_pos + idx];		
				switch (outputSchema[idx].getType() ) {
				case BIGINT : rec.setBigint(idx, Long.parseLong(value));
							break;
				case BOOLEAN: rec.setBoolean(idx, Boolean.parseBoolean(value));
							break;
				case DATETIME: 
							DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
							rec.setDatetime(idx, format.parse(value));
							break;
				case DECIMAL:  rec.setDecimal(idx,  new BigDecimal(value));
							break;
				case DOUBLE: rec.setDouble(idx, Double.parseDouble(value));
							break;
				case STRING: rec.setString(idx, (String)value);
							break;
				default: rec.set(idx, value);
				}
				
			}
		} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		return rec;
	}
		
	public boolean hasNextRecord() throws IOException {
		return reader.readRecord(); 
		
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}
	
}
