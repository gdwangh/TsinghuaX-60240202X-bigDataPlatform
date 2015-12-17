package example;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * Implement ODPS MapReduce to get query result as below:
 *   INSERT OVERWRITE TABLE gby_out
 *   SELECT customer_id, count(*) cnt
 *   FROM orders
 *   GROUP BY customer_id
 *     HAVING cnt >= 3;
 * 
 * Input table schema:
 *   orders (order_id bigint, customer_id string, employee_id string, order_date string)
 * Output table schema:
 *   gby_out (customer_id string, cnt bigint)
 */

public class GroupBy {
	public static class GroupByMapper extends MapperBase {
		private Record customer_id;
		private Record count;
		
		// initial map-key and map-value
		@Override
		public void setup(TaskContext context) throws IOException {
			// map-key:customer_id
			customer_id = context.createMapOutputKeyRecord();
			
			// map-value: cnt
			count = context.createMapOutputValueRecord();
			count.set(new Object[] { 1L });
		}
		
		@Override
		public void map(long key, Record record, TaskContext context) throws IOException {
			// (customer_id, 1)
			customer_id.set(new Object[] { record.get(1).toString() });
			
			context.write(customer_id, count);
		}
	}
	
	public static class GroupByCombiner extends ReducerBase {
		private Record count;

		@Override
		public void setup(TaskContext context) throws IOException {
						count = context.createMapOutputValueRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {			
			Long cnt = 0L;
			
			while (values.hasNext()) {
				Record v = values.next();  
				
				cnt += (Long)v.get(0);
			}
			
			count.set(0,cnt);
			context.write(key, count);

		}
	}
	
	public static class GroupByReducer extends ReducerBase {
		private Record result=null;
		
		@Override
		public void setup(TaskContext context) throws IOException {
			result = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Long cnt = 0L;
			
			while (values.hasNext()) {
				Record v = values.next();  
				
				cnt += (Long)v.get(0);
			}
			
			if (cnt >= 3L) {
				result.set(0,key.get(0));
				result.set(1,cnt);
			
				context.write(result);
			}
			
		}
	}

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: GroupBy <orders> <gby_out>");
      System.exit(-1);
    }
    JobConf job = new JobConf();

    job.setMapperClass(GroupByMapper.class);
    job.setReducerClass(GroupByReducer.class);
    job.setCombinerClass(GroupByCombiner.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("customer_id:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("cnt:bigint"));

    job.setPartitionColumns(new String[] { "customer_id" });
    job.setOutputKeySortColumns(new String[] { "customer_id" });
    job.setOutputGroupingColumns(new String[] { "customer_id" });

    job.setNumReduceTasks(1);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }
}

