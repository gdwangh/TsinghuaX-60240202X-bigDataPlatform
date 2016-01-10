package com.aliyun.odps.mapred;

import java.util.Comparator;

import com.aliyun.odps.data.Record;

public class StringRecordComparator implements Comparator<Record> {

	@Override
	public int compare(Record o1, Record o2) {
		// TODO Auto-generated method stub
		for (int i = 0; i<o1.getColumnCount(); i++) {
			String cv1 = o1.get(i).toString();
			String cv2 = o2.get(i).toString();
			
			if (!cv1.equals(cv2))
				return cv1.compareTo(cv2);
		}
		
		return 0;
	}

}
