package bdma.bigdata.aiwsbu.mapreduce.exo6;

public class exo6Request {

	public static void main(String[] args) {
		
		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class(recuperation nom etudiant)
		HTable table1 = new HTable(config, "A:SIX2");
		String content = "";
		String profName = args[0].toString();

		// Instantiating the Scan class
		Scan scan1 = new Scan();
		RowFilter filtre1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(profName));

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
					byte[] key = r1.getRow();
					byte[] ratio = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));

					String monratio = new String(ratio, "UTF-8");
					String mykey = new String(key, "UTF-8");
					String[] keyPart = mykey.split("/");

					String annee = keyPart[1];
					int year = Integer.valueOf(keyPart[1].toString());
					int newYear = 9999 - year ;

					content = content + "\"" + keyPart[0] + "/" + newYear + "\":{";

					// Instantiating HTable class(recuperation nom du cours)
					HTable table2 = new HTable(config, "A:C");

					// Instantiating the Scan class
					Scan scan2 = new Scan();
					String the_key = cours + "/" + annee;
					scan2.setCacheBlocks(false);
					scan2.withStartRow(the_key.getBytes());
					
					// Scanning the required columns

					scan2.setMaxResultSize(1);
					scan2.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

					// Getting the scan result
					ResultScanner scanner2 = table2.getScanner(scan2);

					for (Result r2 : scanner2) {
						byte[] res2 = r2.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
						String courseName = new String(res2, "UTF-8");
						content = content + "\"Name\":\"" + courseName + "\",";
					}
					
					content = content + "\"Rate\":\"" + monratio + "\"}";
					
					premierpassage = premierpassage - 1;
					
				} else {
					byte[] key = r1.getRow();
					byte[] ratio = r1.getValue(Bytes.toBytes("value"), Bytes.toBytes("test"));

					String monratio = new String(ratio, "UTF-8");
					String mykey = new String(key, "UTF-8");
					String[] keyPart = mykey.split("/");

					String annee = keyPart[1];;
					int year = Integer.valueOf(keyPart[1].toString());
					int newYear = 9999 - year ;

					content = content + ",\"" + keyPart[0] + "/" + newYear + "\":{";
					
					
					// Instantiating HTable class(recuperation nom du cours)
					HTable table3 = new HTable(config, "A:C");

					// Instantiating the Scan class
					Scan scan3 = new Scan();
					String the_key = cours + "/" + annee;
					scan3.setCacheBlocks(false);
					scan3.withStartRow(the_key.getBytes());
					
					// Scanning the required columns

					scan3.setMaxResultSize(1);
					scan3.addColumn(Bytes.toBytes("#"), Bytes.toBytes("N"));

					// Getting the scan result
					ResultScanner scanner3 = table3.getScanner(scan3);

					for (Result r3 : scanner3) {
						byte[] res3 = r3.getValue(Bytes.toBytes("#"), Bytes.toBytes("N"));
						String courseName = new String(res3, "UTF-8");
						content = content + "\"Name\":\"" + courseName + "\",";
					}
					
					content = content + "\"Rate\":\"" + monratio + "\"}";
				}
				
				

			}
			content = content + "}";

			scanner1.close();

		}
		
	}

}
