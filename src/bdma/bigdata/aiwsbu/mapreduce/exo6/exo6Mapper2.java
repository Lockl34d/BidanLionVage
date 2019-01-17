package bdma.bigdata.aiwsbu.mapreduce.exo6;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class exo6Mapper2 extends TableMapper<Text, Text>{
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
			Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException{
			//u/a i
				System.out.println(key);
				System.out.println(value);
				System.out.println(context);
		
		
				String keyStr = key.toString();
				keyStr = keyStr.replaceAll("\\s", "");
				keyStr = decode(keyStr);
				String[] keyPart = keyStr.split("/");
				
				//Text key_res = new Text( keyPart[0]);

				//CellUtil.copyQualifierTo(cell, destination, destinationOffset);
				String res = "";
				System.out.println("-----------------------------");
				for (Cell c :value.listCells()) {
					System.out.println(new String(CellUtil.cloneFamily(c)).equals("I"));
					if (new String(CellUtil.cloneFamily(c)).equals("I")) {
						context.write(new Text(new String(CellUtil.cloneValue(c))), new Text(keyStr));
						System.out.println(new String(CellUtil.cloneValue(c)));						
					}
					//if (new String(CellUtil.cloneQualifier(c)) == "I") {
						//res += new String(CellUtil.cloneValue(c), "UTF-8");						
					//}
				}
				System.out.println("-----------------------------");
				
				//byte[] val = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("I"));
				//String prof = new String(val, "UTF-8");
				
				System.out.println("_____________________________");
				//System.out.println(prof);
				

				
				
				//context.write(new Text(res), new Text(keyPart[0] + "/" + keyPart[1]));
				
				
				
				//s n
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
