package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/*
 * Implement ODPS MapReduce to get query result as below:
 *   INSERT OVERWRITE TABLE join_out
 *   SELECT c.customer_name, o.order_id
 *   FROM customers s LEFT OUTER JOIN orders o 
 *     ON c.customer_id = o.customer_id AND c.country = 'UK';
 *
 * Input table schema:
 *   customers (customer_id string, customer_name string, address string, city string, country string)
 *   orders (order_id bigint, customer_id string, employee_id string, order_date string)
 * Output table schema:
 *   join_out (customer_name string, order_id bigint)
 */
public class LeftOuterJoin {

  public static class LeftOuterJoinMapper extends MapperBase {
  	private Record mapKey;
    private Record mapValue;
    private long tag;

    @Override
    public void setup(TaskContext context) throws IOException {
    	// mapKey: customer_id, tag
      mapKey = context.createMapOutputKeyRecord();
      
      // mapValue: customer_name,country, order_id
      mapValue = context.createMapOutputValueRecord();
      
      tag = context.getInputTableInfo().getLabel().equals("left") ? 0 : 1;
      
    }

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {
   	
    	if (tag == 1) { // record from orders
        mapKey.set(0, record.get(1));   // customer_id
        mapValue.set(2, record.get(0));	// order_id

      } else { // record from customers
        mapKey.set(0, record.get(0));		// customer_id
        mapValue.set(0, record.get(1)); // customer_name
				mapValue.set(1, record.get(4));   // country
      }
      
      mapKey.set(1, tag);
      context.write(mapKey, mapValue);
      
    }
  }

  public static class LeftOuterJoinReducer extends ReducerBase {
		private Record result = null;
		
    @Override
    public void setup(TaskContext context) throws IOException {
    	result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	String customer_id = key.getString(0);
      String customer_name="null";
      String country;
      long tag = 0;
      List<Long> orderIds = new ArrayList<Long>();
      
      // System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"+customer_id);      
      while (values.hasNext()) {
        Record value = values.next();
        tag = key.getBigint(1);
        
        if (tag == 0) {  // from customer
        	customer_name = value.getString(0);
        	country = value.getString(1);
        	
        	System.out.println("**************** "+tag+":"+ customer_id +","+customer_name+","+country+",");

        	if (!country.equals("UK")) {
        		break;       
        	}
        } else {   // from order
        	  // System.out.println("**************** "+tag+":"+ customer_id +","+value.getBigint(2));
        		if (customer_name.equals("null")) break;
        		
        		result.set(0, customer_name);
          	result.set(1, value.getBigint(2));
          	
          	// System.out.println("******** write: ("+result.getString(0)+","+result.getBigint(1));
          	context.write(result);
        }
      }
			
			if (tag == 0) {   // country != "UK" or the customer_id without order
				result.set(0, customer_name);
				result.set(1, null);  
				context.write(result);
			}
    }           		

    private void outputCustomer(String name, TaskContext context) throws IOException {
    }
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: LeftOuterJoin <customers> <orders> <join_out>");
      System.exit(-1);
    }
    JobConf job = new JobConf();

    job.setMapperClass(LeftOuterJoinMapper.class);
    job.setReducerClass(LeftOuterJoinReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("customer_id:string,tag:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("customer_name:string,country:string,order_id:bigint"));

    job.setPartitionColumns(new String[] {"customer_id"});
    job.setOutputKeySortColumns(new String[] {"customer_id", "tag"});
    job.setOutputGroupingColumns(new String[] {"customer_id"});

    job.setNumReduceTasks(1);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).label("left").build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).label("right").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }
}
