package bdma.bigdata.aiwsbu.mapreduce.exo3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class exo3Mapper extends TableMapper<Text, Text>{
	HTable table;
	String last = "";
	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
    	table = new HTable(config, "A:C");
	}
	
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
					throws IOException, InterruptedException {
        //a/s/e/u
		String keyStr = key.toString();
		keyStr = keyStr.replaceAll("\\s", "");
		keyStr = decode(keyStr);
		String[] keyPart = keyStr.split("/");



		byte[] val = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		String note = new String(val, "UTF-8");

		
    	
    	try {
    		Get get = new Get(Bytes.toBytes(keyPart[3]+"/"+(10000-Integer.valueOf(keyPart[0]))));
        	
        	Result result = table.get(get);
        	String name = new String(result.getValue(Bytes.toBytes("#"), Bytes.toBytes("N")));
        	last = name;
        	
        	
		} catch (Exception e) {	}
    	finally {
        	Text key_res = new Text( keyPart[3] + "/" + last);
            //u/name n
            context.write(key_res, new Text(""+ note));
		}
		
		
    	
		
		
		


//		context.write(key_res, new Text(keyStr));


		
	}

	public static String decode(String hexString) {
		int len = hexString.length();
		if (len%2!=0) {
			throw new RuntimeException("bad length");
		}
		StringBuilder sb = new StringBuilder(len/2);
		for (int i=0; i<len; i+=2) {
			final String code = hexString.substring(i, i+2);
			sb.append((char)Integer.parseInt(code, 16));
		}
		return sb.toString();
	}

}
