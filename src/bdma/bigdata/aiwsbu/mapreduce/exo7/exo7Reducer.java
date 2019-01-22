package bdma.bigdata.aiwsbu.mapreduce.exo7;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;


public class exo7Reducer extends TableReducer<Text, Text, ImmutableBytesWritable>{

	public void reduce(Text key, Iterable<Text> val, Context c) throws IOException, InterruptedException {

		
		float res = 0;
		int nb = 0;
		for (Text n : val) {
			Integer note = Integer.valueOf(n.toString());
			res += note / 100.0;
			nb++;
		}
		
		res /= nb;
		DecimalFormat d = new DecimalFormat();
		d.setMaximumFractionDigits(2);
		Float note = Float.parseFloat(d.format(res));
		
		int key_val = (int) (note*100);
		int my_val = 9999-key_val;
		String str_key = key.toString();
		String[] keyPart = str_key.split("/");
		
		
		Put put = new Put(Bytes.toBytes(keyPart[0]+"/"+keyPart[1]+"/"+Integer.toString(my_val)+"/"+keyPart[2]));
		put.add(Bytes.toBytes("value"), Bytes.toBytes("test"),Bytes.toBytes(""+note));
		c.write(null, put);
		

			
		
		
		
	}
	
	
	
	
	
}
