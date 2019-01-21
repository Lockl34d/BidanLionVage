package bdma.bigdata.aiwsbu.mapreduce.exo3;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class exo3 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new exo3(), args);
    	System.out.println(exitCode);
    }
    
    
    
    public int run(String[] args) throws Exception{

    	Configuration config = HBaseConfiguration.create();
    	
    	try {

			HBaseAdmin admin = new HBaseAdmin(config);

			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("A:Exo3"));

			tableDescriptor.addFamily(new HColumnDescriptor("value"));

			admin.createTable(tableDescriptor);			

		} catch (TableExistsException e) {}
    	
    	
    	Job job = new Job(config,"Exo3");
        job.setJarByClass(exo3.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableName tableName = TableName.valueOf("A:G");
        String out = "A:Exo3";
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, exo3Mapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(out, exo3Reducer.class, job);
        
        
        return job.waitForCompletion(true) ? 0 : 1;
    	
    }
     
    
	
	
}
