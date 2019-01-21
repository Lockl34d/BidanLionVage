package bdma.bigdata.aiwsbu.mapreduce.exo5;



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




public class exo5 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new exo5(), args);
    	System.out.println(exitCode);
    }
    
    
    public int run(String[] args) throws Exception{
        /*if (args.length != 2) {
            System.out.printf("Usage: %s <INPUT> <OUTPUT>\n", getClass().getSimpleName());
            return -1;
        }*/
        //Job job = Job.getInstance();
    	
    	Configuration config = HBaseConfiguration.create();
    	
    	try {
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("21502133:Exo5"));
			tableDescriptor.addFamily(new HColumnDescriptor("value"));
			admin.createTable(tableDescriptor);			
		} catch (TableExistsException e) {}


    	
    	
    	Job job = new Job(config,"Exo5");
        job.setJarByClass(exo5.class);
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableName tableName = TableName.valueOf("A:G");
        String out = "21502133:Exo5";
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(LongWritable.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan, exo5Mapper.class, Text.class,
                Text.class, job);
        TableMapReduceUtil.initTableReducerJob(out, exo5Reducer.class, job);
        
        
        return job.waitForCompletion(true) ? 0 : 1;
    	
    }
     
    
	
	
}
