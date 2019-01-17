package bdma.bigdata.aiwsbu.mapreduce.exo6;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class exo6Reducer2 extends TableReducer<Text, Text, ImmutableBytesWritable>{

	public void reduce(Text key, Iterable<Text> val, Context c) throws IOException, InterruptedException {
		
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		for (Text n : val) {
			put.add(Bytes.toBytes("value"), Bytes.toBytes("test"+n),Bytes.toBytes(""+n));
		}
		c.write(null, put);			
		
		
		
	
	}
	
}
