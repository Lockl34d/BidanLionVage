package bdma.bigdata.aiwsbu.mapreduce.exo1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class test extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //int exitCode = ToolRunner.run(new test(), args);
        //System.exit(exitCode);
    	request();
    }
    
    
    public int run(String[] args) throws Exception{
        if (args.length != 2) {
            System.out.printf("Usage: %s <INPUT> <OUTPUT>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        Job job = Job.getInstance();
        job.setJarByClass(test.class);
        job.setJobName("je fais un test de count");
        
        job.setMapperClass(testMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
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
