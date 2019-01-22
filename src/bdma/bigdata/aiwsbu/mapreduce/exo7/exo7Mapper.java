package bdma.bigdata.aiwsbu.mapreduce.exo7;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class exo7Mapper extends TableMapper<Text, Text>{
	HashMap<String,String> semester_to_promo = new HashMap<>();
	
	@Override
	protected void setup(TableMapper<Text, Text>.Context context) throws IOException, InterruptedException {
		semester_to_promo.put("01", "L1");
		semester_to_promo.put("02", "L1");
		semester_to_promo.put("03", "L2");
		semester_to_promo.put("04", "L2");
		semester_to_promo.put("05", "L3");
		semester_to_promo.put("06", "L3");
		semester_to_promo.put("07", "M1");
		semester_to_promo.put("08", "M1");
		semester_to_promo.put("09", "M2");
		semester_to_promo.put("10", "M2");
		
	}
	
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
			//a/s/e/u n
				String keyStr = key.toString();
				keyStr = keyStr.replaceAll("\\s", "");
				keyStr = decode(keyStr);
				String[] keyPart = keyStr.split("/");
				
				byte[] val = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
				String note = new String(val, "UTF-8");
				
				
				Text key_res = new Text( semester_to_promo.get(keyPart[1])+ "/" + keyPart[0] + "/" + keyPart[2]);

				
				
				
				

				
				context.write(key_res, new Text(note));
				
				
				//p/s/e/n n
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
