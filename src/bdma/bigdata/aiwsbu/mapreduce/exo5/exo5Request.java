package bdma.bigdata.aiwsbu.mapreduce.exo5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.io.Text;

public class exo5Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class(recuperation nom etudiant)
	      HTable table1 = new HTable(config, "A:CINQ");
	      
	      String program = args[0].toString();
	      String year = args[1].toString();
	      int int_year = 9999-(Integer.parseInt(year));
    	  String newYear =String.valueOf(int_year);
	      // Instantiating the Scan class
	      Scan scan1 = new Scan();
	      RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(program+"/"+year+"/......."));
	      
	      

	      // Scanning the required columns
	      scan1.setFilter(filtre1);
	      //scan1.setMaxResultSize(1);
	      scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));
	      

	      // Getting the scan result
	      ResultScanner scanner1 = table1.getScanner(scan1);
    	  
	     int temp = 1;
	      System.out.print("{");
	      for(Result r1 : scanner1) {
	    	  
	    	  byte[] byte_idmatiere = r1.getRow();
	    	  byte[] byte_note = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
	    	  
	    	  String string_idmatiere = new String (byte_idmatiere,"UTF-8");
	    	  String[] keyPart = string_idmatiere.split("/");
	    	  String note = new String(byte_note,"UTF-8");
	    	  String idmatiere = keyPart[2];
	    	  // Instantiating HTable class(recuperation nom du cours)
		      HTable table3 = new HTable(config, "A:C");

		      // Instantiating the Scan class
		      String key = idmatiere+"/"+newYear;
		      Scan scan3 = new Scan();
		      scan3.withStartRow(key.getBytes());
		      scan3.setCacheBlocks(false);
		      //RowFilter filtre3 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(idmatiere+"/["+newYear.charAt(0)+"-9]["+newYear.charAt(1)+"-9]["+newYear.charAt(2)+"-9]["+newYear.charAt(3)+"-9]") );
		      //scan3.setFilter(filtre3);
		      

		      // Scanning the required columns
		      scan3.setMaxResultSize(1);
		      //scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

		      // Getting the scan result
		      ResultScanner scanner3 = table3.getScanner(scan3);
		      
		      
		      
		      
		      
		      
		      
		      
		    	  if(temp ==1) {
			    	  byte[] res3 = scanner3.next().getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
			    	  String name = new String(res3, "UTF-8");
			    	  System.out.print("\""+idmatiere+"\":{\"Name\":\""+name+"\",\"Rate\":\""+note+"\"}"); 
			    	  temp--;
		    	  }else {
			    	  byte[] res3 = scanner3.next().getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
			    	  String name = new String(res3, "UTF-8");
			    	  System.out.print(",\""+idmatiere+"\":{\"Name\":\""+name+"\",\"Rate\":\""+note+"\"}"); 
		    		  
		    	  }

	    	  

	      }
	      System.out.print("}");
	      
	      scanner1.close();
	    

	}

}

