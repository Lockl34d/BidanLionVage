package bdma.bigdata.aiwsbu.mapreduce.exo1;

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

public class exo1Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class
	      HTable table = new HTable(config, "A:R");

	      // Instantiating the Scan class
	      Scan scan = new Scan();
	      RowFilter filtre = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("2001000154/L2/.{7}/..") );

	      // Scanning the required columns
	      scan.setFilter(filtre);
	      scan.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));

	      // Getting the scan result
	      ResultScanner scanner = table.getScanner(scan);

	      // Reading values from scan result
	      //for (Result result = scanner.next(); result != null; result = scanner.next())
	      
	      for(Result r : scanner) {

	    	  byte[] key = r.getRow();
	    	  
	    	  String mykey = new String(key, "UTF-8");

	    	  mykey = mykey.replaceAll("\\s", "");
	    	  String[] keyPart = mykey.split("/");
	    	  String numEtu = keyPart[0];
	    	  String cours = keyPart[2];
	    	  String annee = keyPart[4];
	    	  //System.out.println(cours);
	    	  byte[] res = r.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
	    	  String note = new String(res, "UTF-8");
	    	  
	    	  //--------------------------------------------------------------------------------
	    	  
	    	  int putain = 9999-(Integer.parseInt(annee));
	    	  String newYear =String.valueOf(putain);
	    			  
	    	  
		      // Instantiating HTable class(recuperation nom etudiant)
		      HTable table3 = new HTable(config, "A:S");

		      // Instantiating the Scan class
		      Scan scan3 = new Scan();
		      RowFilter filtre3 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(numEtu));
		      
		      

		      // Scanning the required columns
		      scan3.setFilter(filtre3);
		      scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("F"));
		      scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("L"));
		      scan3.addColumn(Bytes.toBytes("C"), Bytes.toBytes("E"));
		      scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("P"));

		      // Getting the scan result
		      ResultScanner scanner3 = table3.getScanner(scan3);
	    	  
		      for(Result r3 : scanner3) {
		    	  byte[] res3 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("F"));
		    	  byte[] res3_2 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("L"));
		    	  byte[] res3_3 = r3.getValue(Bytes.toBytes("C"), Bytes.toBytes("E"));
		    	  byte[] res3_4 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("P"));
		    	  String name = new String(res3, "UTF-8") + " " + new String(res3_2, "UTF-8");
		    	  //System.out.println(res3.toString());
		    	  System.out.println(name + " " + new String(res3_3, "UTF-8") + " " + new String(res3_4, "UTF-8"));
		    	

		    	  // Instantiating HTable class(recuperation nom du cours)
			      HTable table2 = new HTable(config, "A:C");

			      // Instantiating the Scan class
			      Scan scan2 = new Scan();
			      RowFilter filtre2 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(cours+"/["+newYear.charAt(0)+"-9]["+newYear.charAt(1)+"-9]["+newYear.charAt(2)+"-9]["+newYear.charAt(3)+"-9]") );
			      
			      

			      // Scanning the required columns
			      scan2.setFilter(filtre2);
			      scan2.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

			      // Getting the scan result
			      ResultScanner scanner2 = table2.getScanner(scan2);
		    	  
			      for(Result r2 : scanner2) {
			    	  byte[] res2 = r2.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
			    	  //System.out.println(res2.toString());
			    	  System.out.println(new String(res2, "UTF-8"));
		    	  
			      }
		      }
		      
		  
	    	  
	    	  
	    	  System.out.println("Found : "+mykey+" "+ note + cours);
	      }

	      //System.out.println("Found row : " + result);
	      //closing the scanner
	      scanner.close();

	}

}
