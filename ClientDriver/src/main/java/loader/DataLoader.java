package loader;

import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by li-me on 29/9/2016.
 */
public class DataLoader {
	private static Session session;
	private static Cluster cluster;

	private static void log(String info) {
		System.out.println("LOG INFO: " + info);
	}

	private static void createKeySpace() {
		// create for d8 and d40

		session.execute("CREATE KEYSPACE IF NOT EXISTS d8Cas WITH replication"
				+ " ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1};");

		session.execute("CREATE KEYSPACE IF NOT EXISTS d40Cas WITH replication"
				+ " ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1};");
		log("Finished Creating KeySpace");
	}

	private static void createWareHouseTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.warehouse("
				+ "W_ID int," + "W_YTD double," + "PRIMARY KEY (W_ID));");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.warehouse("
				+ "W_ID int," + "W_YTD double," + "PRIMARY KEY (W_ID));");
		log("Finished Creating WareHouse");

	}

	private static void createWareHouseUnchangedTable() {
		session.execute("CREATE TABLE IF NOT EXISTS d8Cas.warehouse_unchanged("
				+ "W_ID int," + " W_STREET_1 VARCHAR," + " W_STREET_2 VARCHAR,"
				+ "W_CITY VARCHAR," + "W_STATE VARCHAR," + "W_ZIP VARCHAR,"
				+ "W_TAX DOUBLE," + "W_NAME VARCHAR," + "PRIMARY KEY (W_ID));");
		session.execute("CREATE TABLE IF NOT EXISTS d40Cas.warehouse_unchanged("
				+ "W_ID int,"
				+ " W_STREET_1 VARCHAR,"
				+ " W_STREET_2 VARCHAR,"
				+ "W_CITY VARCHAR,"
				+ "W_STATE VARCHAR,"
				+ "W_ZIP VARCHAR,"
				+ "W_TAX DOUBLE," + "W_NAME VARCHAR," + "PRIMARY KEY (W_ID));");
		log("Finished Creating WareHouseUnchangedTable");
	}

	private static void createDistrictTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.district("
				+ "D_W_ID int," + "D_ID int," + "D_NEXT_O_ID int,"
				+ "D_YTD double," + "PRIMARY KEY (D_W_ID, D_ID)"
				+ ") WITH CLUSTERING ORDER BY (D_ID ASC);");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.district("
				+ "D_W_ID int," + "D_ID int," + "D_NEXT_O_ID int,"
				+ "D_YTD double," + "PRIMARY KEY (D_W_ID, D_ID)"
				+ ") WITH CLUSTERING ORDER BY (D_ID ASC);");
		log("Finished Creating DistrictTable");

	}

	private static void createDistrictUnchangedTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.district_unchanged("
				+ "D_ID int," + "D_W_ID int," + "D_NAME VARCHAR,"
				+ "D_STREET_1 VARCHAR," + "D_STREET_2 VARCHAR,"
				+ "D_CITY VARCHAR," + "D_STATE VARCHAR," + "D_ZIP VARCHAR,"
				+ "PRIMARY KEY (D_W_ID, D_ID)"
				+ ") WITH CLUSTERING ORDER BY (D_ID ASC);");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.district_unchanged("
				+ "D_ID int,"
				+ "D_W_ID int,"
				+ "D_NAME VARCHAR,"
				+ "D_STREET_1 VARCHAR,"
				+ "D_STREET_2 VARCHAR,"
				+ "D_CITY VARCHAR,"
				+ "D_STATE VARCHAR,"
				+ "D_ZIP VARCHAR,"
				+ "PRIMARY KEY (D_W_ID, D_ID)"
				+ ") WITH CLUSTERING ORDER BY (D_ID ASC);");
		log("Finished Creating DistrictUnchanged");
	}

	private static void createStockTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.stocks("
				+ "S_W_ID int," + "S_I_ID int," + "S_QTY int,"
				+ "S_ORDER_CNT int," + "S_REMOTE_CNT int," + "S_YTD double,"
				+ "PRIMARY KEY (S_W_ID, S_I_ID)"
				+ ") WITH CLUSTERING ORDER BY (S_I_ID ASC);");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.stocks("
				+ "S_W_ID int," + "S_I_ID int," + "S_QTY int,"
				+ "S_ORDER_CNT int," + "S_REMOTE_CNT int," + "S_YTD double,"
				+ "PRIMARY KEY (S_W_ID, S_I_ID)"
				+ ") WITH CLUSTERING ORDER BY (S_I_ID ASC);");
		log("Finished Creating StockTable");
	}

	private static void createItemTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.item(" + "I_ID int,"
				+ "I_NAME text," + "I_PRICE double," + "PRIMARY KEY (I_ID));");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.item("
				+ "I_ID int," + "I_NAME text," + "I_PRICE double,"
				+ "PRIMARY KEY (I_ID));");
		log("Finished Creating Item Table");

	}

	private static void createOrderTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.orders("
				+ "O_W_ID int," + "O_D_ID int," + "O_ID int,"
				+ "O_CARRIER_ID int," + "O_ENTRY_D timestamp,"
				+ "O_OL_CNT int," + "O_C_ID int,"
				+ "PRIMARY KEY (O_W_ID, O_D_ID,O_ID,O_ENTRY_D)" + ") ;");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.orders("
				+ "O_W_ID int," + "O_D_ID int," + "O_ID int,"
				+ "O_CARRIER_ID int," + "O_ENTRY_D timestamp,"
				+ "O_OL_CNT int," + "O_C_ID int,"
				+ "PRIMARY KEY (O_W_ID, O_D_ID,O_ID,O_ENTRY_D)" + ") ;");
		log("Finished Creating Order Table");

	}

	private static void createOrderLineTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.order_line("
				+ "OL_W_ID int," + "OL_D_ID int," + "OL_O_ID int,"
				+ "OL_NUMBER int," + "OL_DELIVERY_D int,"
				+ "OL_SUPPLY_W_ID int," + "OL_I_ID int,"
				+ "OL_QUANTITY double," + "OL_AMOUNT double,"
				+ "PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.order_line("
				+ "OL_W_ID int," + "OL_D_ID int," + "OL_O_ID int,"
				+ "OL_NUMBER int," + "OL_DELIVERY_D int,"
				+ "OL_SUPPLY_W_ID int," + "OL_I_ID int,"
				+ "OL_QUANTITY double," + "OL_AMOUNT double,"
				+ "PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));");
		log("Finished Creating OrderLine");
	}

	private static void createCustomerTable() {
		session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.customer("
				+ "C_W_ID int," + "C_D_ID int," + "C_ID int,"
				+ "C_FIRST VARCHAR," + "C_MIDDLE VARCHAR," + "C_LAST VARCHAR,"
				+ "C_STREET_1 VARCHAR," + "C_STREET_2 VARCHAR,"
				+ "C_CITY VARCHAR," + "C_STATE VARCHAR," + "C_ZIP VARCHAR,"
				+ "C_PHONE VARCHAR," + "C_SINCE TIMESTAMP,"
				+ "C_CREDIT_LIM DOUBLE," + "C_BALANCE DOUBLE,"
				+ "C_PAYMENT_CNT int," + "C_CREDIT VARCHAR,"
				+ "C_DISCOUNT DOUBLE," + "C_YTD_PAYMENT double,"
				+ "W_TAX DOUBLE," + "D_TAX DOUBLE," + "W_NAME VARCHAR,"
				+ "D_NAME VARCHAR," + "PRIMARY KEY (C_W_ID, C_D_ID, C_ID));");
		session.execute("CREATE TABLE IF NOT EXISTS  d40Cas.customer("
				+ "C_W_ID int," + "C_D_ID int," + "C_ID int,"
				+ "C_FIRST VARCHAR," + "C_MIDDLE VARCHAR," + "C_LAST VARCHAR,"
				+ "C_STREET_1 VARCHAR," + "C_STREET_2 VARCHAR,"
				+ "C_CITY VARCHAR," + "C_STATE VARCHAR," + "C_ZIP VARCHAR,"
				+ "C_PHONE VARCHAR," + "C_SINCE TIMESTAMP,"
				+ "C_CREDIT_LIM DOUBLE," + "C_BALANCE DOUBLE,"
				+ "C_PAYMENT_CNT int," + "C_CREDIT VARCHAR,"
				+ "C_DISCOUNT DOUBLE," + "C_YTD_PAYMENT double,"
				+ "W_TAX DOUBLE," + "D_TAX DOUBLE," + "W_NAME VARCHAR,"
				+ "D_NAME VARCHAR," + "PRIMARY KEY (C_W_ID, C_D_ID, C_ID));");
		log("Finished Creating CustomerTable");
	}

	private static void insertWareHouseTable(String nodeIP, String keyspace,
			String path) {
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String warehouse = path + "/" + "warehouse.csv";
		String input = null;
		String queryW, queryU;
		BufferedReader br = null;
		String[] list;

		try {
			FileReader fr = new FileReader(warehouse);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				String ytd = list[8].split("\\.")[0];
				int wytd = Integer.parseInt(ytd);
				int wid = Integer.parseInt(list[0]);
				double tax = Double.parseDouble(list[7]);
				for (int i = 0; i < 7; i++) {
					list[i] = "'" + list[i] + "'";
				}

				queryW = "INSERT INTO warehouse(W_ID,W_YTD)" + " VALUES(" + wid
						+ "," + wytd + ");";
				queryU = "INSERT INTO warehouse_unchanged(W_ID,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_TAX,W_NAME)"
						+ " VALUES("
						+ wid
						+ ","
						+ list[2]
						+ ","
						+ list[3]
						+ ","
						+ list[4]
						+ ","
						+ list[5]
						+ ","
						+ list[6]
						+ ","
						+ tax + "," + list[1] + ");";
				session.execute(queryW);
				log("WareHouseTable: " + wid + ":" + wytd);
				session.execute(queryU);
				log("WareHouseUnchangedTable: " + wid + ":" + list[2] + ":"
						+ list[3] + ":" + list[4] + ":" + list[5] + ":"
						+ list[6] + ":" + tax + ":" + list[1]);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + warehouse);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + warehouse);
		}

		cluster.close();
		log("WareHouseTable and WareHouseUnchangedTable are inserted.");
	}

	private static void insertDistrictTable(String nodeIP, String keyspace,
			String path) {
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String district = path + "/" + "district.csv";
		String input = null;
		String queryD, queryDU;
		BufferedReader br = null;
		String[] list;

		try {
			FileReader fr = new FileReader(district);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				int dwid = Integer.parseInt(list[0]);
				int did = Integer.parseInt(list[1]);
				int dnextoid = Integer.parseInt(list[10]);
				String ytd = list[9].split("\\.")[0];
				int dytd = Integer.parseInt(ytd);
				queryD = "INSERT INTO district(D_W_ID,D_ID,D_NEXT_O_ID,D_YTD)"
						+ " VALUES(" + dwid + "," + did + "," + dnextoid + ","
						+ dytd + ");";
				log("QueryD is " + queryD);
				for (int i = 0; i < 8; i++) {
					list[i] = "'" + list[i] + "'";
				}
				queryDU = "INSERT INTO district_unchanged(D_ID,D_W_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP)"
						+ " VALUES("
						+ did
						+ ","
						+ dwid
						+ ","
						+ list[2]
						+ ","
						+ list[3]
						+ ","
						+ list[4]
						+ ","
						+ list[5]
						+ ","
						+ list[6] + "," + list[7] + ");";
				session.execute(queryD);
				log("DistrictTable: " + dwid + ":" + did + ":" + dnextoid + ":"
						+ dytd);
				session.execute(queryDU);
				log("DistrictUnchangedTable: " + did + ":" + dwid + ":"
						+ list[2] + ":" + list[3] + ":" + list[4] + ":"
						+ list[5] + ":" + list[6] + ":" + list[7]);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + district);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + district);
		}

		cluster.close();
		log("DistrictTable and DistrictUnchangedTable are inserted.");
	}

	private static void insertStockTable(String nodeIP, String keyspace,
			String path) {
		/*
		 * session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.stocks(" +
		 * "S_W_ID int," + "S_I_ID int," + "S_QTY int," + "S_ORDER_CNT int," +
		 * "S_REMOTE_CNT int," + "S_YTD double," +
		 * "PRIMARY KEY (S_W_ID, S_I_ID)" +
		 * ") WITH CLUSTERING ORDER BY (S_I_ID ASC);");
		 */
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String stock = path + "/" + "stock.csv";
		String input = null;
		String queryD, queryDU;
		BufferedReader br = null;
		String[] list;
		try {
			FileReader fr = new FileReader(stock);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				int s_w_id = Integer.parseInt(list[0]);
				int s_i_id = Integer.parseInt(list[1]);
				double qty = Double.parseDouble(list[2]);
				double ytd = Double.parseDouble(list[3]);
				double order_cnt = Integer.parseInt(list[4]);
				double remote_order_cnt = Integer.parseInt(list[5]);
				queryD = "INSERT INTO stocks(S_W_ID,S_I_ID,S_QTY,S_ORDER_CNT, S_REMOTE_CNT, S_YTD)"
						+ " VALUES("
						+ s_w_id
						+ ","
						+ s_i_id
						+ ","
						+ qty
						+ ","
						+ order_cnt + "," + remote_order_cnt + "," + ytd + ");";
				log("QueryD is " + queryD);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + stock);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + stock);
		}
		session.close();
		cluster.close();
		log("Stock insertion finished.");
	}

	private static void insertItemTable(String nodeIP, String keyspace,
			String path) {
		/*
		 * session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.item(" +
		 * "I_ID int," + "I_W_ID int," + "I_NAME text," + "I_PRICE double," +
		 * "PRIMARY KEY (I_W_ID, I_ID));");
		 */
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String item = path + "/" + "item.csv";
		String input = null;
		String queryD, queryDU;
		BufferedReader br = null;
		String[] list;
		try {
			FileReader fr = new FileReader(item);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				int i_id = Integer.parseInt(list[0]);
				String i_name = "'" + list[1] + "'";
				double i_price = Double.parseDouble(list[2]);
				queryD = "INSERT INTO item(I_ID, I_NAME, I_PRICE)" + " VALUES("
						+ i_id + "," + i_name + "," + i_price + ");";
				log("QueryD is " + queryD);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + item);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + item);
		}
		session.close();
		cluster.close();
		log("Item insertion finished.");
	}

	private static void insertOrderTable(String nodeIP, String keyspace,
			String path) {
		/*
		 * session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.orders(" +
		 * "O_W_ID int," + "O_D_ID int," + "O_ID int," + "O_CARRIER_ID int," +
		 * "O_ENTRY_D timestamp," + "O_OL_CNT int," + "O_C_ID int," +
		 * "PRIMARY KEY (O_W_ID, O_D_ID,O_ID,O_ENTRY_D)" + ") ;");
		 */
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String order = path + "/" + "order.csv";
		String input = null;
		String queryD, queryDU;
		BufferedReader br = null;
		String[] list;
		try {
			FileReader fr = new FileReader(order);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				int o_w_id = Integer.parseInt(list[0]);
				int o_d_id = Integer.parseInt(list[1]);
				int o_id = Integer.parseInt(list[2]);
				int o_c_id = Integer.parseInt(list[3]);
				if (list[4].equals("null") || list[4].equals("\"null\"")) {
					list[4] = "-1";
				}

				int o_carrier_id = Integer.parseInt(list[4]);
				int o_ol_cnt = Integer.parseInt(list[5]);
				list[6].replace('/', '-');
				String time = "'" + list[7] + "'";
				queryD = "INSERT INTO orders(O_W_ID, O_D_ID, O_ID, O_CARRIER_ID, O_ENTRY_D, O_OL_CNT, O_C_ID)"
						+ " VALUES("
						+ o_w_id
						+ ","
						+ o_d_id
						+ ","
						+ o_id
						+ ","
						+ o_carrier_id
						+ ","
						+ time
						+ ","
						+ o_ol_cnt
						+ ","
						+ o_c_id + ");";
				log("QueryD is " + queryD);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + order);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + order);
		}
		session.close();
		cluster.close();
		log("Orders insertion finished.");
	}

	private static void insertOrderLineTable(String nodeIP, String keyspace,
			String path) {
		/*
		 * session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.order_line(" +
		 * "OL_W_ID int," + "OL_D_ID int," + "OL_O_ID int," + "OL_NUMBER int," +
		 * "OL_DELIVERY_D int," + "OL_SUPPLY_W_ID int," + "OL_I_ID int," +
		 * "OL_QUANTITY int," + "OL_AMOUNT int," +
		 * "PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER));");
		 */
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String order_line = path + "/" + "order-line.csv";
		String input = null;
		String queryD, queryDU;
		BufferedReader br = null;
		String[] list;
		try {
			FileReader fr = new FileReader(order_line);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				list = input.trim().replaceAll("\\s", "").split(",");
				int ol_w_id = Integer.parseInt(list[0]);
				int ol_d_id = Integer.parseInt(list[1]);
				int ol_o_id = Integer.parseInt(list[2]);
				int ol_number = Integer.parseInt(list[3]);
				int ol_i_id = Integer.parseInt(list[4]);
				list[5].replace("/", "-");
				String time = "'" + list[5] + "'";
				double ol_amount = Double.parseDouble(list[6]);
				int ol_supply_w_id = Integer.parseInt(list[7]);
				double ol_quantity = Double.parseDouble(list[8]);
				queryD = "INSERT INTO orders(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_DELIVERY_D, OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT)"
						+ " VALUES("
						+ ol_w_id
						+ ","
						+ ol_d_id
						+ ","
						+ ol_o_id
						+ ","
						+ ol_number
						+ ","
						+ time
						+ ","
						+ ol_supply_w_id
						+ ","
						+ ol_i_id
						+ ","
						+ ol_quantity
						+ ","
						+ ol_amount
						+ ");";
				log("QueryD is " + queryD);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + order_line);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + order_line);
		}
		session.close();
		cluster.close();
		log("Orders insertion finished.");
	}

	private static void insertCustomerTable(String nodeIP, String keyspace,
			String path) {
		/* session.execute("CREATE TABLE IF NOT EXISTS  d8Cas.customer("
		 *+ "C_W_ID int," + "C_D_ID int," + "C_ID int,"
		 *+ "C_FIRST VARCHAR," + "C_MIDDLE VARCHAR," + "C_LAST VARCHAR,"
		 *+ "C_STREET_1 VARCHAR," + "C_STREET_2 VARCHAR,"
		 *+ "C_CITY VARCHAR," + "C_STATE VARCHAR," + "C_ZIP VARCHAR,"
		 *+ "C_PHONE VARCHAR," + "C_SINCE TIMESTAMP,"
		 *+ "C_CREDIT_LIM DOUBLE," + "C_BALANCE DOUBLE,"
		 *+ "C_PAYMENT_CNT int," + "C_CREDIT VARCHAR,"
		 *+ "C_DISCOUNT DOUBLE," + "C_YTD_PAYMENT double,"
		 *+ "W_TAX DOUBLE," + "D_TAX DOUBLE," + "W_NAME VARCHAR,"
		 *+ "D_NAME VARCHAR," + "PRIMARY KEY (C_W_ID, C_D_ID, C_ID));");
		 */
		cluster = Cluster.builder().addContactPoint(nodeIP).build();
		session = cluster.connect(keyspace);

		String customer = path + "/" + "customer.csv";
		String warehouse = path + "/" + "warehouse.csv";
		String district = path + "/" + "district.csv";
		String input = null;
		String queryC;
		Map<Integer, Double> wTaxMap = new HashMap<Integer, Double>();
		Map<Integer, Double> dTaxMap = new HashMap<Integer, Double>();
		Map<Integer, String> wNameMap = new HashMap<Integer, String>();
		Map<Integer, String> dNameMap = new HashMap<Integer, String>();
		BufferedReader br = null;
		String[] listC,listW,listD;
		//get the w_tax and w_name from warehouse and insert into two maps
		try {
			FileReader fr = new FileReader(warehouse);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				listW = input.trim().replaceAll("\\s", "").split(",");
				int w_id = Integer.parseInt(listW[0]);
				double w_tax = Double.parseDouble(listW[7]);
				String w_name = "'" + listW[1] + "'";
				wTaxMap.put(w_id,w_tax);
				wNameMap.put(w_id,w_name);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + warehouse);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + warehouse);
		}
		//get the d_tax and d_name from district insert them into two maps
		try {
			FileReader fr = new FileReader(district);
			br = new BufferedReader(fr);
			input = br.readLine();

			while (input != null) {
				listD = input.trim().replaceAll("\\s", "").split(",");
				int d_id = Integer.parseInt(listD[1]);
				double d_tax = Double.parseDouble(listD[8]);
				String d_name = "'" + listD[2] + "'";
				dTaxMap.put(d_id,d_tax);
				dNameMap.put(d_id,d_name);
				input = br.readLine();
			}

			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + district);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + district);
		}
		//start to insert into the customer table
		try {
			FileReader fr = new FileReader(customer);
			br = new BufferedReader(fr);
			input = br.readLine();
			
			while (input != null) {
				listC = input.trim().replaceAll("\\s", "").split(",");
				int c_w_id = Integer.parseInt(listC[0]);
				int c_d_id = Integer.parseInt(listC[1]);
				int c_id = Integer.parseInt(listC[2]);
				String c_first = "'" + listC[3] + "'";
				String c_middle = "'" + listC[4] + "'";
				String c_last = "'" + listC[5] + "'";
				String c_street_1 = "'" + listC[6] + "'";
				String c_street_2 = "'" + listC[7] + "'";
				String c_city = "'" + listC[8] + "'";
				String c_state = "'" + listC[9] + "'";
				String c_zip = "'" + listC[10] + "'";
				String c_phone = "'" + listC[11] + "'";
				String c_since = "'" + listC[12] + "'";
				double c_credit_lim = Double.parseDouble(listC[14]);
				double c_balance = Double.parseDouble(listC[16]);
				int c_payment_cnt = Integer.parseInt(listC[18]);
				String c_credit = "'" + listC[13] + "'";
				double c_discount = Double.parseDouble(listC[15]);
				double c_ytd_payment = Double.parseDouble(listC[17]);
				double w_tax = wTaxMap.get(c_w_id);
				double d_tax = dTaxMap.get(c_d_id);
				String w_name = wNameMap.get(c_w_id);
				String d_name = dNameMap.get(c_d_id);
				queryC = "INSERT INTO customer(C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT_LIM, C_BALANCE, C_PAYMENT_CNT, C_CREDIT, C_DISCOUNT, C_YTD_PAYMENT, W_TAX, D_TAX, W_NAME, D_NAME)"
						+ " VALUES("
						+ c_w_id
						+ ","
						+ c_d_id
						+ ","
						+ c_id
						+ ","
						+ c_first
						+ ","
						+ c_middle
						+ ","
						+ c_last
						+ ","
						+ c_street_1
						+ ","
						+ c_street_2
						+ ","
						+ c_city
						+ ","
						+ c_state
						+ ","
						+ c_zip
						+ ","
						+ c_phone
						+ ","
						+ c_since
						+ ","
						+ c_credit_lim
						+ ","
						+ c_balance
						+ ","
						+ c_payment_cnt
						+ ","
						+ c_credit
						+ ","
						+ c_discount
						+ ","
						+ c_ytd_payment
						+ ","
						+ w_tax
						+ ","
						+ d_tax
						+ ","
						+ w_name
						+ ","
						+ d_name
						+ ");";
				
				log("QueryC is " + queryC);
				input = br.readLine();
			}
			
			br.close();
		} catch (FileNotFoundException ex) {
			log("Cannot find the file: " + customer);
		} catch (IOException ce) {
			log("There are some errors when reading the file: " + customer);
		}
		session.close();
		cluster.close();
		log("Customer's insertion finished.");
	}

	private static void dropAllTables() {
	}

	private static void init(String nodeIP) {
		cluster = Cluster.builder().addContactPoints(nodeIP).build();
		session = cluster.connect();
	}

	private static void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		String nodeIP = "127.0.0.1";
		String keyspaceD8 = "d8Cas";
		String keyspaceD40 = "d40Cas";
		String pathD8 = "/temp/team05/D8data";
		String pathD40 = "/temp/team05/D40data";

		DataLoader.init(nodeIP);
		// DataLoader.createKeySpace();
		// DataLoader.createWareHouseTable();
		// DataLoader.createWareHouseUnchangedTable();
		// DataLoader.createDistrictTable();
		// DataLoader.createDistrictUnchangedTable();
		// DataLoader.createStockTable();
		// DataLoader.createItemTable();
		// DataLoader.createOrderTable();
		//DataLoader.createOrderLineTable();
		DataLoader.createCustomerTable();
		DataLoader.close();
		// DataLoader.insertWareHouseTable(nodeIP,keyspaceD8,pathD8);
		// DataLoader.insertWareHouseTable(nodeIP,keyspaceD40,pathD40);
		// DataLoader.insertDistrictTable(nodeIP,keyspaceD8,pathD8);
		// DataLoader.insertDistrictTable(nodeIP,keyspaceD40,pathD40);
		// DataLoader.insertStockTable(nodeIP,keyspaceD8,pathD8);
		// DataLoader.insertStockTable(nodeIP,keyspaceD40,pathD40);
		// DataLoader.insertItemTable(nodeIP,keyspaceD8,pathD8);
		// DataLoader.insertItemTable(nodeIP,keyspaceD40,pathD40);
		// DataLoader.insertOrderTable(nodeIP,keyspaceD8,pathD8);
		// DataLoader.insertOrderTable(nodeIP,keyspaceD40,pathD40);
		// DataLoader.insertOrderLineTable(nodeIP, keyspaceD8, pathD8);
		// DataLoader.insertOrderLineTable(nodeIP,keyspaceD40,pathD40);
		DataLoader.insertCustomerTable(nodeIP, keyspaceD8, pathD8);
		DataLoader.insertCustomerTable(nodeIP, keyspaceD40, pathD40);
	}
}
