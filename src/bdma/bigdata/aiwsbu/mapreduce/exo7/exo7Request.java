package bdma.bigdata.aiwsbu.mapreduce.exo7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class exo7Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class(recuperation nom etudiant)
	      HTable table1 = new HTable(config, "A:Exo7");
	      
	      String promo = args[0].toString();
	      String annee = args[1].toString();

	      // Instantiating the Scan class
	      Scan scan1 = new Scan();
	      RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(promo+"/"+annee+"/.*"));
	      
	      

	      // Scanning the required columns
	      scan1.setFilter(filtre1);
	      scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));
	      

	      // Getting the scan result
	      ResultScanner scanner1 = table1.getScanner(scan1);

	      Result scanner10 = table1.getScanner(scan1).next();
	      if(scanner10==null) {
	    	  System.out.print("NOT FOUND");
	      }else {
		      System.out.print("{");
		      int temp = 1;
		      for(Result r1 : scanner1) {
		    	  if (temp==1) {
			    	  byte[] key = r1.getRow();
			    	  byte[] res = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
			    	  String note = new String(res, "UTF-8");
			    	  String mykey = new String(key, "UTF-8");
	
			    	  mykey = mykey.replaceAll("\\s", "");
			    	  String[] keyPart = mykey.split("/");
			    	  
			    	  String etu = keyPart[3];
			    	  System.out.print("\""+etu+"\":\""+note+"\"");
			    	  temp--;
				}
		    	  else {
			    	  byte[] key = r1.getRow();
			    	  byte[] res = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
			    	  String note = new String(res, "UTF-8");
			    	  String mykey = new String(key, "UTF-8");
	
			    	  mykey = mykey.replaceAll("\\s", "");
			    	  String[] keyPart = mykey.split("/");
			    	  
			    	  String etu = keyPart[3];
			    	  System.out.print(",\""+etu+"\":\""+note+"\"");
		    	  }
	
		    	  
		    	  
		      }
		      System.out.print("}");
	      }
	}

}
