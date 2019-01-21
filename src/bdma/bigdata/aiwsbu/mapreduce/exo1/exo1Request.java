package bdma.bigdata.aiwsbu.mapreduce.exo1;

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

public class exo1Request {

	public static void main(String[] args) throws IOException {
	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class(recuperation nom etudiant)
	      HTable table1 = new HTable(config, "A:S");
	      
	      String numEtu = args[0].toString();
	      String program = args[1].toString();

	      // Instantiating the Scan class
	      Scan scan1 = new Scan();
	      RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(numEtu));
	      
	      

	      // Scanning the required columns
	      scan1.setFilter(filtre1);
	      scan1.addColumn(Bytes.toBytes("#"), Bytes.toBytes("F"));
	      scan1.addColumn(Bytes.toBytes("#"), Bytes.toBytes("L"));
	      scan1.addColumn(Bytes.toBytes("C"), Bytes.toBytes("E"));
	      scan1.addColumn(Bytes.toBytes("#"), Bytes.toBytes("P"));
	      

	      // Getting the scan result
	      ResultScanner scanner1 = table1.getScanner(scan1);
    	  
	      for(Result r1 : scanner1) {
	    	  byte[] keyEtu = r1.getRow();
	    	  byte[] res1 = r1.getValue(Bytes.toBytes("#"), Bytes.toBytes("F"));
	    	  byte[] res1_2 = r1.getValue(Bytes.toBytes("#"), Bytes.toBytes("L"));
	    	  byte[] res1_3 = r1.getValue(Bytes.toBytes("C"), Bytes.toBytes("E"));
	    	 
	    	  
	    	  String name = new String(res1, "UTF-8") + " " + new String(res1_2, "UTF-8");
	    	  String email = new String(res1_3, "UTF-8");
	    	  
	    	  //System.out.println(res3.toString());
	    	  //System.out.println(name + " " + new String(res1_3, "UTF-8") + " " + new String(res1_4, "UTF-8"));
	    	  //HashMap <String, > releve = new HashMap();
	    	  // creer liste First et Second
	    	  ArrayList <ArrayList<String>>first = new ArrayList<ArrayList<String>>();
	    	  ArrayList <ArrayList<String>>second = new ArrayList<ArrayList<String>>();
	    	 
	    	  
	    	  // Instantiating HTable class
		      HTable table2 = new HTable(config, "A:Exo1");

		      // Instantiating the Scan class
		      Scan scan2 = new Scan();
		      RowFilter filtre2 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(numEtu+"/"+program+"/.*/.*/.*") );
		      

		      // Scanning the required columns
		      scan2.setFilter(filtre2);
		      scan2.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));

		      // Getting the scan result
		      ResultScanner scanner2 = table2.getScanner(scan2);

		      // Reading values from scan result
		      //for (Result result = scanner.next(); result != null; result = scanner.next())
		      
		      for(Result r : scanner2) {

		    	  byte[] key = r.getRow();
		    	  
		    	  String mykey = new String(key, "UTF-8");

		    	  mykey = mykey.replaceAll("\\s", "");
		    	  String[] keyPart = mykey.split("/");
		    	  String promo = keyPart[1];
		    	  String cours = keyPart[2];
		    	  String semestre = keyPart[3];
		    	  String annee = keyPart[4];
		    	  //System.out.println(cours);
		    	  byte[] res = r.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
		    	  String note = new String(res, "UTF-8");
		    	  
		    	  //--------------------------------------------------------------------------------
		    	  
		    	  int year = 9999-(Integer.parseInt(annee));
		    	  String newYear =String.valueOf(year);
		    	  
		    	  ArrayList <String>infoUE = new ArrayList<String>();
		    	 
		    	  
		    	  // Instantiating HTable class(recuperation nom du cours)
			      HTable table3 = new HTable(config, "A:C");

			      // Instantiating the Scan class
			      Scan scan3 = new Scan();
			      String the_key = cours+"/"+newYear;
			      //RowFilter filtre3 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(cours+"/["+newYear.charAt(0)+"-9]["+newYear.charAt(1)+"-9]["+newYear.charAt(2)+"-9]["+newYear.charAt(3)+"-9]") );
			      scan3.setCacheBlocks(false);
			      scan3.withStartRow(the_key.getBytes());
			      

			      // Scanning the required columns
			      //scan3.setFilter(filtre3);
			      scan3.setMaxResultSize(1);
			      scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

			      // Getting the scan result
			      ResultScanner scanner3 = table3.getScanner(scan3);
		    	  
			      for(Result r3 : scanner3) {
			    	  byte[] res3 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
			    	  //System.out.println(res2.toString());
			    	  //System.out.println(new String(res3, "UTF-8"));
			    	  infoUE.add(cours);
			    	  infoUE.add(new String(res3, "UTF-8"));
			    	  infoUE.add(note);
		    	  
			      }
			      if(Integer.parseInt(semestre)%2 == 1)
			    	  first.add(infoUE);
			      else
			    	  second.add(infoUE);
			   
		      
		      }
	    	
		      System.out.print("{\"Name\":\"" + name + "\",\"Email\":\"" + email + "\",\"Program\":\"" + program+"\",");
		      System.out.print("\"First\":[");
		      for(int j = 0; j<first.size(); j++)
		      {
		    	  
		    	  if(j<first.size()-2) {
			    	  for(int i = 0; i<first.get(j).size(); i++) {
			    		  if(i==0) {
			    			  System.out.print("{\"Code\":\""+first.get(j).get(0)+"\",");
			    		  }
			    		  if(i==1) {
			    			  System.out.print("\"Name\":\""+first.get(j).get(1)+"\",");
			    		  }
			    		  if(i==2) {
			    			  System.out.print("\"Grade\":\""+first.get(j).get(2)+"\"},");
			    		  }
			    	  }
		    	  }else {
			    	  for(int i = 0; i<first.get(j).size(); i++) {
			    		  if(i==0) {
			    			  System.out.print("{\"Code\":\""+first.get(j).get(0)+"\",");
			    		  }
			    		  if(i==1) {
			    			  System.out.print("\"Name\":\""+first.get(j).get(1)+"\",");
			    		  }
			    		  if(i==2) {
			    			  System.out.print("\"Grade\":\""+first.get(j).get(2)+"\"}],");
			    		  }
			    	  }		    		  

		    	  }
		      }
		      
		      
		      
		      
		      System.out.print("\"Second\":[");
		      for(int j = 0; j<second.size(); j++)
		      {
		    	  
		    	  if(j<second.size()-1) {
			    	  for(int i = 0; i<second.get(j).size(); i++) {
			    		  if(i==0) {
			    			  System.out.print("{\"Code\":\""+second.get(j).get(0)+"\",");
			    		  }
			    		  if(i==1) {
			    			  System.out.print("\"Name\":\""+second.get(j).get(1)+"\",");
			    		  }
			    		  if(i==2) {
			    			  System.out.print("\"Grade\":\""+second.get(j).get(2)+"\"},");
			    		  }
			    	  }
		    	  }else {
			    	  for(int i = 0; i<second.get(j).size(); i++) {
			    		  if(i==0) {
			    			  System.out.print("{\"Code\":\""+second.get(j).get(0)+"\",");
			    		  }
			    		  if(i==1) {
			    			  System.out.print("\"Name\":\""+second.get(j).get(1)+"\",");
			    		  }
			    		  if(i==2) {
			    			  System.out.print("\"Grade\":\""+second.get(j).get(2)+"\"}]}");
			    		  }
			    	  }		    		  

		    	  }
		      }
	      }
	      
	      scanner1.close();
	    

	}

}