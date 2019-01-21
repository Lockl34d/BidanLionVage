package bdma.bigdata.aiwsbu.mapreduce.exo6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class nameMapReduce {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
    	
		try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("A:Names"));
			tableDescriptor.addFamily(new HColumnDescriptor("N"));
			admin.createTable(tableDescriptor);			
		} catch (TableExistsException e) {}
    	
		
		Job job = new Job(config,"Names");
        job.setJarByClass(nameMapReduce.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableName tableName = TableName.valueOf("A:C");
        String out = "A:Names";
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, nameMapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(out, nameReducer.class, job);
        
        
        System.out.println(job.waitForCompletion(true));
	}
}
