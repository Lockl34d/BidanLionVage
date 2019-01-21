package bdma.bigdata.aiwsbu.mapreduce.exo2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class exo2 extends Configured {

	private static final String table1name = "A:DEUX1";
	
    public static void main(String[] args) throws Exception {
        System.out.println(p1() && p2());
    	
    }
    
    
    public static boolean p1() throws Exception{
    	Configuration config = HBaseConfiguration.create();
    	////////////
    	HBaseAdmin admin = new HBaseAdmin(config);
    	HTableDescriptor table = new HTableDescriptor("A:DEUX1");
    	table.addFamily(new HColumnDescriptor("value"));
    	try {
    		admin.createTable(table);
		} catch (TableExistsException e) {} // if table exist exception is catched and throwed away.
    	///////////
    	Job job = new Job(config,"Exo2-1");
        job.setJarByClass(exo2.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob("A:G", scan, exo2Mapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(table1name, exo2Reducer.class, job);
        return job.waitForCompletion(true);
    }
    
    
    public static boolean p2() throws Exception{
    	Configuration config = HBaseConfiguration.create();
    	Job job = new Job(config,"Exo2-2");
        job.setJarByClass(exo2.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob("A:DEUX1", scan, exo2Mapper2.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob("A:Exo2", exo2Reducer2.class, job);
        
        
        return job.waitForCompletion(true);
    	
    }
     
    
	
	
}
