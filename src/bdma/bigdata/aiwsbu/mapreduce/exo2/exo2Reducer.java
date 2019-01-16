package bdma.bigdata.aiwsbu.mapreduce.exo2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.sun.xml.internal.ws.util.StreamUtils;

public class exo2Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	public void reduce(Text key, Iterable<Text> val, Context c) throws IOException, InterruptedException {

		
		int sum = 0;
		int size = 0;
		for (Text n : val) {
			Integer note = Integer.valueOf(n.toString());
			sum += note;
			size++;
		}
		int res = sum / size;
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("value"), Bytes.toBytes("test"), Bytes.toBytes(""+res));
		c.write(null, put);
		

			
		
		
		
	}
	
	
	
	
	
}
