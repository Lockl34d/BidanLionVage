package bdma.bigdata.aiwsbu.mapreduce.exo3;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class exo3Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	public void reduce(Text key, Iterable<Text> val, Context c) throws IOException, InterruptedException {
		//u/name n
		
		float res = 0;
		int nb = 0;
		for (Text n : val) {
			Integer note = Integer.valueOf(n.toString());
			res += note > 1000 ? 1 : 0;
			nb++;
		}
		res /= nb;
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("value"), Bytes.toBytes("test"),Bytes.toBytes(""+res));
		c.write(null, put);
		

			
		
		
		
	}
	
	
	
	
	
}
