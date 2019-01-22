package bdma.bigdata.aiwsbu.mapreduce.exo4;

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

public class exo4Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class(recuperation nom etudiant)
	      HTable table1 = new HTable(config, "A:Exo4");
	      
	      String idmatiere = args[0].toString();
	      String year = args[1].toString();
	      int int_year = 9999-(Integer.parseInt(year));
    	  String newYear =String.valueOf(int_year);
	      // Instantiating the Scan class
	      Scan scan1 = new Scan();
	      RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(idmatiere+"/"+year));
	      
	      

	      // Scanning the required columns
	      scan1.setFilter(filtre1);
	      scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));
	      

	      // Getting the scan result
	      ResultScanner scanner1 = table1.getScanner(scan1);
    	  
	      Result r = table1.getScanner(scan1).next();
	      if (r==null) {
			System.out.print("NOT FOUND");
	      }else {
	      
	      for(Result r1 : scanner1) {
	    	  

	    	  byte[] byte_note = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
	    	  String note = new String(byte_note,"UTF-8");
	    	  // Instantiating HTable class(recuperation nom du cours)
		      HTable table3 = new HTable(config, "A:C");

		      // Instantiating the Scan class
		      Scan scan3 = new Scan();
		      String my_key = idmatiere+"/"+newYear;
		      //RowFilter filtre3 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(idmatiere+"/["+newYear.charAt(0)+"-9]["+newYear.charAt(1)+"-9]["+newYear.charAt(2)+"-9]["+newYear.charAt(3)+"-9]") );
		      
		      

		      // Scanning the required columns
		      //scan3.setFilter(filtre3);
		      scan3.withStartRow(my_key.getBytes());
		      scan3.setCacheBlocks(false);
		      scan3.setMaxResultSize(1);
		      scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

		      // Getting the scan result
		      ResultScanner scanner3 = table3.getScanner(scan3);
		      String name = new String(scanner3.next().getValue(Bytes.toBytes("#"), Bytes.toBytes("N")), "UTF-8");
	    	  System.out.println("{\"Name\":\""+name+"\",\"Rate\":\""+note+"\"}");
	    	  
		      /*for(Result r3 : scanner3) {
		    	  byte[] res3 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
		    	  String name = new String(res3, "UTF-8");
	    	  
		      }*/

	      }
	      }
	      
	      scanner1.close();
	    

	}

}

