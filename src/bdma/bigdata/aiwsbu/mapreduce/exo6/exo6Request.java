package bdma.bigdata.aiwsbu.mapreduce.exo6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class exo6Request {

	public static void main(String[] args) throws IOException {
		
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class(recuperation nom etudiant)
		HTable table1 = new HTable(config, "A:Exo62");
		String content = "";
		String profName = "Beeo Ncbgflh";

		// Instantiating the Scan class
		Scan scan1 = new Scan();
		RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(profName + "/.*"));

		// Scanning the required columns
		scan1.setFilter(filtre1);
		scan1.addColumn(Bytes.toBytes("value"), Bytes.toBytes("test"));

		// Getting the scan result
		ResultScanner scanner1 = table1.getScanner(scan1);
		Result test = table1.getScanner(scan1).next();
		if (test == null) {
			content = "NOT FOUND";
		}
	    
		else {

			content = "{";

			int premierpassage = 1;

			for (Result r1 : scanner1) {

				if (premierpassage == 1) {
					
					byte[] newKey = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));

					String courseYear = new String(newKey, "UTF-8");
					
					String[] keyPart = courseYear.split("/");

					String annee = keyPart[1];
					int year = Integer.valueOf(keyPart[1].toString());
					int newYear = 9999 - year ;

					content = content + "\"" + keyPart[0] + "/" + newYear + "\":{";

					// Instantiating HTable class(recuperation nom du cours)
					HTable table2 = new HTable(config, "A:C");

					// Instantiating the Scan class
					Scan scan2 = new Scan();
					String the_key = keyPart[0] + "/" + annee;
					
					scan2.withStartRow(the_key.getBytes());
					
					// Scanning the required columns
					scan2.setCacheBlocks(false);
					scan2.setMaxResultSize(1);
					scan2.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

					// Getting the scan result
					ResultScanner scanner2 = table2.getScanner(scan2);
					content = content + "\"Name\":\"" + new String(scanner2.next().getValue(Bytes.toBytes("#"), Bytes.toBytes("N")), "UTF-8") + "\",";

					
					// Instantiating HTable class(recuperation rate)
					HTable table2_1 = new HTable(config, "A:Exo61");

					// Instantiating the get class
					
					String the_key_1 = newYear + "/" + keyPart[0];
					Get g = new Get(Bytes.toBytes(the_key_1));
					
					// Getting the get result
					Result get = table2_1.get(g);
					
					byte[] value = get.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
					String rate = new String(value, "UTF-8");
					
					
					content = content + "\"Rate\":\"" + rate + "\"}";
					System.out.println(content);
					premierpassage = premierpassage - 1;
					
				} else {
					byte[] newKey = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));

					String courseYear = new String(newKey, "UTF-8");
					
					String[] keyPart = courseYear.split("/");

					String annee = keyPart[1];
					int year = Integer.valueOf(keyPart[1].toString());
					int newYear = 9999 - year ;
					System.out.println(newYear);

					

					// Instantiating HTable class(recuperation nom du cours)
					HTable table2 = new HTable(config, "A:C");

					// Instantiating the Scan class
					Scan scan2 = new Scan();
					String the_key = keyPart[0] + "/" + annee;
					
					scan2.withStartRow(the_key.getBytes());
					
					// Scanning the required columns
					scan2.setCacheBlocks(false);
					scan2.setMaxResultSize(1);
					scan2.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

					// Getting the scan result
					ResultScanner scanner2 = table2.getScanner(scan2);
					

					
					// Instantiating HTable class(recuperation rate)
					HTable table2_1 = new HTable(config, "A:Exo61");

					// Instantiating the get class
					
					String the_key_1 = newYear + "/" + keyPart[0];
					Get g = new Get(Bytes.toBytes(the_key_1));
					
					// Getting the get result
					Result get = table2_1.get(g);
					if(get.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"))!=null) {
						byte[] value = get.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));
						String rate = new String(value, "UTF-8");
						content = content + ",\"" + keyPart[0] + "/" + newYear + "\":{";
						content = content + "\"Name\":\"" + new String(scanner2.next().getValue(Bytes.toBytes("#"), Bytes.toBytes("N")), "UTF-8") + "\",";
						content = content + "\"Rate\":\"" + rate + "\"}";
					}
				}
				
				

			}
			content = content + "}";

			scanner1.close();

		}
		System.out.println(content);
	}

}
