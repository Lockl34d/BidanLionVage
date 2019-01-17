package bdma.bigdata.aiwsbu.mapreduce.exo2;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class exo2Mapper extends TableMapper<Text, Text>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//a/s/e/u n
		String keyStr = key.toString();
		keyStr = keyStr.replaceAll("\\s", "");
		keyStr = decode(keyStr);
		String[] keyPart = keyStr.split("/");

		Text key_res = new Text( keyPart[1]+"/"+keyPart[0]+"/"+keyPart[2]);


		byte[] val = value.getValue(Bytes.toBytes("#"), Bytes.toBytes("G"));
		String note = "";
		try {
			note = new String(val, "UTF-8");
			
		} catch (Exception e) {
			System.out.println("-----------------------------");
			System.out.println(val);
			System.out.println(val);
			System.out.println("-----------------------------");
		}

		context.write(key_res, new Text(note));
		//s/a/e n
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
