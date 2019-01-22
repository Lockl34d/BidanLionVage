package bdma.bigdata.aiwsbu.mapreduce.exo3;




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



public class exo3Request {



	public static void main(String[] args) throws IOException {

		// Instantiating Configuration class

		Configuration config = HBaseConfiguration.create();



		// Instantiating HTable class(recuperation nom etudiant)

		HTable table1 = new HTable(config, "A:Exo3");



		String eu = args[0];

		// Instantiating the Scan class

		Scan scan1 = new Scan();

		RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(eu+"/.*"));


		// Scanning the required columns

		scan1.setFilter(filtre1);

		scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));





		// Getting the scan result

		ResultScanner scanner1 = table1.getScanner(scan1);




		System.out.print("[");
		boolean first = true;
		for(Result r1 : scanner1) {
			if (!first) System.out.print(",");
			byte[] byte_key = r1.getRow();

			byte[] byte_note = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));

			String string_keya = new String (byte_key,"UTF-8");

			String[] keyPart = string_keya.split("/");

			String note = new String(byte_note,"UTF-8");

			String name ="DEFAULT NAME : EU NOT NAMED YET";
			try {
				name = keyPart[1];
				System.out.print("{\"Name\":\""+name+"\",\"Rate\":\""+note+"\"}"); 
				first = false;
			} catch (Exception e) {}


		}

		System.out.print("]");



		scanner1.close();





	}



}