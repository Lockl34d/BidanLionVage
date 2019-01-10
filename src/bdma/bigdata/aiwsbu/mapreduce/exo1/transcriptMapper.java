package bdma.bigdata.aiwsbu.mapreduce.exo1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class transcriptMapper extends Mapper<Text, Text, Text, Text>{
	HashMap<String,String> semester_to_promo = new HashMap<>();
	
	@Override
	protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		semester_to_promo.put("S01", "L1");
		semester_to_promo.put("S02", "L1");
		semester_to_promo.put("S03", "L2");
		semester_to_promo.put("S04", "L2");
		semester_to_promo.put("S05", "L3");
		semester_to_promo.put("S06", "L3");
		semester_to_promo.put("S07", "M1");
		semester_to_promo.put("S08", "M1");
		semester_to_promo.put("S09", "M2");
		semester_to_promo.put("S10", "M2");
		
	}
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//a/s/e/u n
		String keyStr = key.toString();
		String[] keyPart = keyStr.split("/");
		
		Text key_res = new Text( keyPart[2] + "/" + semester_to_promo.get(keyPart[1]) + "/" + keyPart[3] + "/" + keyPart[1]);
		context.write(key_res, value);
		//e/p/u/s n
	}
}
