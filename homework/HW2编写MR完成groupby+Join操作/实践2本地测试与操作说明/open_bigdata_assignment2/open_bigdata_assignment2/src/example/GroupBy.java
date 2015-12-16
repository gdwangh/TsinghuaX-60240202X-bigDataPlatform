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

public class GroupBy {
	public static class GroupByMapper extends MapperBase {
		private Record mapKey;
		private Record mapValue;
		
		// initial map-key and map-value
		@Override
		public void setup(TaskContext context) throws IOException {
			// map-key:customer_id
			mapKey = context.createMapOutputKeyRecord();
			
			// map-value: cnt
			mapValue = context.createMapOutputValueRecord();
		}
		
		@Override
		public void map(long key, Record record, TaskContext context) throws IOException {
			// (customer_id, 1)
			Long cnt = 1L;
			
			mapKey.set(0, record.get(1));
			mapValue.set(0, cnt);
		
			context.write(mapKey, mapValue);
		}
	}
	
	public static class GroupByCombiner extends ReducerBase {
		private Record result = null;

		@Override
		public void setup(TaskContext context) throws IOException {
						result = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			String customer_id = key.getString(0);
			
			Long cnt = 0L;
			
			while (values.hasNext()) {
				Long v = values.next().getBigint(0);
				
				cnt += v;
			}
			
			result.set(0, customer_id);
			result.set(1, cnt);
			
			if (cnt >= 3L) {
				context.write(result);
			}
		}
	}
	
	public static class GroupByReducer extends ReducerBase {
		private Record result = null;
		
		@Override
		public void setup(TaskContext context) throws IOException {
			result = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			String customer_id = key.getString(0);
			
			Long cnt = 0L;
			
			while (values.hasNext()) {
				Long v = values.next().getBigint(0);
				
				cnt += v;
			}
			
			result.set(0, customer_id);
			result.set(1, cnt);
			
			if (cnt >= 3L) {
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
