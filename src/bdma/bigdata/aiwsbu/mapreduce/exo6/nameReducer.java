package bdma.bigdata.aiwsbu.mapreduce.exo6;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class nameReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context c)
			throws IOException, InterruptedException {
		
		int min = 10000;
		String res = "";
		for (Text value : values) {
			String valuePart[] = value.toString().split("/");
			int annee = Integer.valueOf(valuePart[0]);
			if (annee<min) {
				min = annee;
				res = valuePart[1];
			}
			
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("N"), Bytes.toBytes("test"), Bytes.toBytes(""+res));
		c.write(null, put);

	}
}
