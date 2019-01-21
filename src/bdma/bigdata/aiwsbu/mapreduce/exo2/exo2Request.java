package bdma.bigdata.aiwsbu.mapreduce.exo2;

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

public class exo2Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class(recuperation nom etudiant)
	      HTable table1 = new HTable(config, "A:Exo2");
	      
	      String semestre = args[0].toString();

	      // Instantiating the Scan class
	      Scan scan1 = new Scan();
	      RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(semestre+"/...."));
	      
	      

	      // Scanning the required columns
	      scan1.setFilter(filtre1);
	      scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));
	      

	      // Getting the scan result
	      ResultScanner scanner1 = table1.getScanner(scan1);
    	  if(scanner1.next()==null) {
    		  System.out.print("NOT FOUND");
    	  }else {
	      
	      System.out.print("[");
	    
	      int premierpassage = 1;
	      
	      for(Result r1 : scanner1) {
	    	  
	    	  if(premierpassage==1) {
		    	  byte[] key = r1.getRow();
		    	  byte[] ratio = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
		    	 
		    	  
		    	  String monratio = new String(ratio, "UTF-8");
		    	  String mykey = new String(key,"UTF-8");
		  		  String[] keyPart = mykey.split("/");
					
		  		  String annee = keyPart[1];
		  		  
		  		  System.out.print("{\"Year\":\""+annee+"\",\"Rate\":\""+monratio+"\"}"); 
	    		  
		  		  premierpassage = premierpassage-1;
	    	  }else {
	    	  byte[] key = r1.getRow();
	    	  byte[] ratio = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
	    	 
	    	  
	    	  String monratio = new String(ratio, "UTF-8");
	    	  String mykey = new String(key,"UTF-8");
	  		  String[] keyPart = mykey.split("/");
				
	  		  String annee = keyPart[1];
	  		  
	  		  System.out.print(",{\"Year\":\""+annee+"\",\"Rate\":\""+monratio+"\"}");
	    	  } 

	      }
	      System.out.print("]");
	      
	      scanner1.close();
	    
    	  }
	}

}

