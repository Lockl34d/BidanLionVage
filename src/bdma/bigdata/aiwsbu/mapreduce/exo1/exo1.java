package bdma.bigdata.aiwsbu.mapreduce.exo1;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class exo1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new exo1(), args);
    	System.out.println(exitCode);
    }
    
    
    public int run(String[] args) throws Exception{
        
    	Configuration config = HBaseConfiguration.create();
    	

    	try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("A:Exo1"));
			tableDescriptor.addFamily(new HColumnDescriptor("value"));
			admin.createTable(tableDescriptor);	
		} catch (TableExistsException e) {}		

    	Job job = new Job(config,"Note");
        job.setJarByClass(exo1.class);
        //job.setJobName("je fais un test de count");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableName tableName = TableName.valueOf("A:G");
        String out = "A:Exo1";
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, exo1Mapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(out, exo1Reducer.class, job);
        
        
        return job.waitForCompletion(true) ? 0 : 1;
    	
    }
     
    public static void request() throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        //Filter filter = new  RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*/.*/2001000102/.*"));
        Scan scan = new Scan();
        TableName tableName = TableName.valueOf("A:G");
        Table table = connection.getTable(tableName);
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result scannerResult : scanner) {
                System.out.println("Scan: " + scannerResult);
            }
        }       
    }
	
	
}