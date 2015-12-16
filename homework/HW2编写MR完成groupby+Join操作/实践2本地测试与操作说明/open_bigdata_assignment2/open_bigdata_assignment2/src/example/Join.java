package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 *   INSERT OVERWRITE TABLE join_example_out
 *   SELECT o.order_id, c.customer_name 
 *   FROM orders o JOIN customers c 
 *        ON o.customer_id = c.customer_id;
 *
 * Input table schema:
 *   orders (order_id bigint, customer_id string, employee_id string, order_date string)
 *   customers (customer_id string, customer_name string, address string, city string, country string)
 * Output table schema:
 *   join_example_out (order_id bigint, customer_name string)
 *
 * @author Xie Dejun <dejun.xiedj@alibaba-inc.com>
 */
public class Join {
  public static final Log LOG = LogFactory.getLog(Join.class);

  public static class JoinMapper extends MapperBase {
    private Record mapKey;
    private Record mapValue;
    private long tag;

    @Override
    public void setup(TaskContext context) 
        throws IOException {
      // mapKey: customer_id, tag
      mapKey = context.createMapOutputKeyRecord();
      // mapValue: order_id, customer_name
      mapValue = context.createMapOutputValueRecord();
      tag = context.getInputTableInfo().getLabel().equals("left") ? 0 : 1;
    }

    @Override
    public void map(long key, Record record, 
        TaskContext context) throws IOException {
      if (tag == 0) { // record from orders
        mapKey.set(0, record.get(1));
        mapValue.set(0, record.get(0));
      } else { // record from customers
        mapKey.set(0, record.get(0));
        mapValue.set(1, record.get(1));
      }
      mapKey.set(1, tag);
      context.write(mapKey, mapValue);
    }
  }

  public static class JoinReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) 
        throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, 
        TaskContext context) throws IOException {
      String customer_id = key.getString(0);
      List<Long> orderIds = new ArrayList<Long>();
      while (values.hasNext()) {
        Record value = values.next();
        long tag = key.getBigint(1);
        if (tag == 0) {
          orderIds.add(value.getBigint(0));
        } else {
          for (long orderId: orderIds) {
            result.set(0, orderId);
            result.set(1, value.getString(1));
            context.write(result);
          }
        }
      }
    }
  }

  public static void main(String[] args) 
      throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: Join <orders> <customers> <join_example_out>");
      System.exit(-1);
    }
    JobConf job = new JobConf();

    job.setMapperClass(JoinMapper.class);
    job.setReducerClass(JoinReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("customer_id:string,tag:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("order_id:bigint,customer_name:string"));

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
