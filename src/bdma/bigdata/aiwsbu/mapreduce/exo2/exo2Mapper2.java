package bdma.bigdata.aiwsbu.mapreduce.exo2;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class exo2Mapper2 extends TableMapper<Text, Text>{
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
			throws IOException, InterruptedException {
		//s/a/e n
		String keyStr = key.toString();
		keyStr = keyStr.replaceAll("\\s", "");
		keyStr = decode(keyStr);
		String[] keyPart = keyStr.split("/");

		Text key_res = new Text( keyPart[0]+"/"+keyPart[1]);


		byte[] val = value.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
		String note = new String(val, "UTF-8");

		context.write(key_res, new Text(note));
		//s/a n
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
