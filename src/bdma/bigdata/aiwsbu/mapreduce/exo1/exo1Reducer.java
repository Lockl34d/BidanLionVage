package bdma.bigdata.aiwsbu.mapreduce.exo1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class exo1Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	public void reduce(Text key, Iterable<Text> val, Context c) throws IOException, InterruptedException {
		for(Text value : val) {
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("value"),Bytes.toBytes("test"),Bytes.toBytes(value.toString()));
			c.write(null, put);
		}

			
		
		
		
	}
	
	
	
	
	
}
