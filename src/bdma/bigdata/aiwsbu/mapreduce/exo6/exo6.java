package bdma.bigdata.aiwsbu.mapreduce.exo6;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

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

import bdma.bigdata.aiwsbu.mapreduce.exo5.exo5Mapper;
import bdma.bigdata.aiwsbu.mapreduce.exo5.exo5Reducer;




public class exo6 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //int exitCode = ToolRunner.run(new exo6(), args);
    	
    	System.out.println(p2() && p1());
    }
    
    
    public int run(String[] args) throws Exception{
        /*if (args.length != 2) {
            System.out.printf("Usage: %s <INPUT> <OUTPUT>\n", getClass().getSimpleName());
            return -1;
        }*/
        //Job job = Job.getInstance();
    	
    	Configuration config = HBaseConfiguration.create();
    	Job job = new Job(config,"Exo6-1");
        TableName tableName = TableName.valueOf("A:G");
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("A:G", scan, exo6Mapper1.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("A:SIX1", exo6Reducer1.class, job);
        
        
        
        TableMapReduceUtil.initTableMapperJob("A:C", scan, exo6Mapper2.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("A:SIX2", exo6Reducer2.class, job);
        
    	return job.waitForCompletion(true) ? 0: 1;
    }
    
    private static boolean p1() throws Exception {
    	Configuration config = HBaseConfiguration.create();
    	try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("A:SIX1"));
			tableDescriptor.addFamily(new HColumnDescriptor("value"));
			admin.createTable(tableDescriptor); 
    	} catch (TableExistsException e) {} 
    	Job job = new Job(config,"Exo6-1");
    	Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("A:G", scan, exo6Mapper1.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("A:SIX1", exo6Reducer1.class, job);
        return job.waitForCompletion(true);
    }
    
    private static boolean p2() throws Exception {
    	Configuration config = HBaseConfiguration.create();
    	try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("A:SIX2"));
			tableDescriptor.addFamily(new HColumnDescriptor("value"));
			admin.createTable(tableDescriptor); 
    	} catch (TableExistsException e) {} 
    	
    	Job job = new Job(config,"Exo6-2");
    	Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("A:C", scan, exo6Mapper2.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("A:SIX2", exo6Reducer2.class, job);
        return job.waitForCompletion(true);
    }
	
	
}
