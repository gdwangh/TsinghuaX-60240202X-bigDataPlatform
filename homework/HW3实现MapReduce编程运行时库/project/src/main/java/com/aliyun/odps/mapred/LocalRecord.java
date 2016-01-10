package com.aliyun.odps.mapred;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;

public class LocalRecord implements Record {
	ArrayList<Column> columnDef;
	ArrayList<Object> columnValues;
	
	LocalRecord() {
		columnDef = new ArrayList<Column>();
		columnValues = new ArrayList<Object>();
	}

	
	LocalRecord(Column[] colList) {
		columnDef = new ArrayList<Column>();
		columnValues = new ArrayList<Object>();
				
		if (colList != null)  {
			for (int i=0; i<colList.length; i++) {
				columnDef.add(colList[i]);
				columnValues.add(null);
			}
		}
	}
	
	LocalRecord(LocalRecord r) {
		columnDef = new ArrayList<Column>();
		columnValues = new ArrayList<Object>();
		
		this.columnDef.addAll(r.columnDef);
		this.columnValues.addAll(r.columnValues);
	}
	
	@Override
	public Object get(int index) {
		// TODO Auto-generated method stub
		return this.columnValues.get(index);		
	}

	@Override
	public Object get(String columnName) {
		// TODO Auto-generated method stub
		int index;
		for (index=0; index < this.columnDef.size(); index++) {
			if (columnName.equals(this.columnDef.get(index).getName())) {
				return this.columnValues.get(index);
			}
		}
		return null;
	}

	@Override
	public Long getBigint(int index) {
		// TODO Auto-generated method stub
		return (Long)this.get(index);
	}

	@Override
	public Long getBigint(String name) {
		// TODO Auto-generated method stub
		return (Long)this.get(name);
	}

	@Override
	public Boolean getBoolean(int index) {
		// TODO Auto-generated method stub
		return (Boolean)this.get(index);
	}

	@Override
	public Boolean getBoolean(String name) {
		// TODO Auto-generated method stub
		return (Boolean)this.get(name);
	}

	@Override
	public byte[] getBytes(int index) {
		// TODO Auto-generated method stub
		return (byte[])this.get(index);
	}

	@Override
	public byte[] getBytes(String name) {
		// TODO Auto-generated method stub
		return (byte[])this.get(name);
	}

	@Override
	public int getColumnCount() {
		// TODO Auto-generated method stub
		return this.columnDef.size();
	}

	@Override
	public Column[] getColumns() {
		// TODO Auto-generated method stub
		Column[] ret = new Column[this.columnDef.size()];
		
		return this.columnDef.toArray(ret);
	}

	@Override
	public Date getDatetime(int index) {
		// TODO Auto-generated method stub
		return (Date)this.get(index);
	}

	@Override
	public Date getDatetime(String name) {
		// TODO Auto-generated method stub
		return (Date)this.get(name);
	}

	@Override
	public BigDecimal getDecimal(int index) {
		// TODO Auto-generated method stub
		return (BigDecimal)this.get(index);
	}

	@Override
	public BigDecimal getDecimal(String name) {
		// TODO Auto-generated method stub
		return (BigDecimal)this.get(name);
	}

	@Override
	public Double getDouble(int index) {
		// TODO Auto-generated method stub
		return (Double)this.get(index);
	}

	@Override
	public Double getDouble(String name) {
		// TODO Auto-generated method stub
		return (Double)this.get(name);
	}

	@Override
	public String getString(int index) {
		// TODO Auto-generated method stub
		return (String)this.get(index);
	}

	@Override
	public String getString(String name) {
		// TODO Auto-generated method stub
		return (String)this.get(name);
	}

	@Override
	public void set(Object[] value) {
		// TODO Auto-generated method stub
		for (int i=0; i<this.columnDef.size(); i++) {
			set(i, value[i]);
		}
	}

	@Override
	public void set(int index, Object value) {
		// TODO Auto-generated method stub
		switch (this.columnDef.get(index).getType() ) {
		case BIGINT : this.setBigint(index, (Long)value);
					break;
		case BOOLEAN: this.setBoolean(index, (Boolean)value);
					break;
		case DATETIME: this.setDatetime(index, (Date)value);
					break;
		case DECIMAL:  this.setDecimal(index, (BigDecimal)value);
					break;
		case DOUBLE: this.setDouble(index, (Double)value);
					break;
		case STRING: this.setString(index, (String)value);
					break;
		default: this.columnValues.add(index, value);
		}
		
	}

	@Override
	public void set(String name, Object value) {
		// TODO Auto-generated method stub
		int idx;
		for (idx=0; idx<this.columnDef.size(); idx++) {
			if (name.equals(this.columnDef.get(idx).getName()) ) {
				this.set(idx, value);
				break;
			}
		}
	}

	@Override
	public void setBigint(int index, Long value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setBigint(String name, Long value) {
		// TODO Auto-generated method stub
		set(name, value);
	}

	@Override
	public void setBoolean(int index, Boolean value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setBoolean(String name, Boolean value) {
		// TODO Auto-generated method stub
		this.set(name, value);

	}

	@Override
	public void setDatetime(int index, Date value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setDatetime(String name, Date value) {
		// TODO Auto-generated method stub
		this.set(name, value);

	}

	@Override
	public void setDecimal(int index, BigDecimal value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setDecimal(String name, BigDecimal value) {
		// TODO Auto-generated method stub
		this.set(name, value);
	
	}

	@Override
	public void setDouble(int index, Double value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	
	}

	@Override
	public void setDouble(String name, Double value) {
		// TODO Auto-generated method stub
		this.set(name, value);
	
	}

	@Override
	public void setString(int index, String value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setString(String name, String value) {
		// TODO Auto-generated method stub
		this.set(name, value);
	
	}

	@Override
	public void setString(int index, byte[] value) {
		// TODO Auto-generated method stub
		this.columnValues.set(index, value);
	}

	@Override
	public void setString(String name, byte[] value) {
		// TODO Auto-generated method stub
		this.set(name, value);
	
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return this.columnValues.toArray();
	}

	// return the index of column to be added just
	public int addColumn(String ColumnName,  OdpsType type) {
		Column newCol = new Column(ColumnName, type);
		this.columnDef.add(newCol);
		this.columnValues.add(null);
		
		int index = this.columnDef.size() - 1;
		return index;
	}
	
	public void addColumns(Column[] cols) {
		for (int idx=0; idx<cols.length; idx++) {
			this.columnDef.add(cols[idx]);
			this.columnValues.add(null);
		}
	}
}
