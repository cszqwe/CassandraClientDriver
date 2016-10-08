package clientdriver;

import transactions.*;

import com.datastax.driver.core.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;


public class ClientDriver {
    private String database, fileName;
    private int totalNumXact=0;
    private long totalTime=0;
    private Delivery deliveryObj;
    private NewOrder newOrderObj;
    private OrderStatus orderStatusObj;
    private Payment paymentObj;
    private PopularItem popularItemObj;
    private StockLevel stockLevelObj;
    private TopBalance topBalanceObj;
    private Session session;
    private Cluster cluster;
    public static void main(String[] args) {
        System.out.println("Hi, I am the client driver");
        // para1-> database(D8/D40), para2-> fileName(0...)
        if (args.length == 3){
            String para1 = args[0];
            String para2 = args[1];
            String para3 = args[2];
            try{
                ClientDriver driver = new ClientDriver(para1, para2, para3);
                driver.readTransactions();
            }catch(Exception e){
                e.printStackTrace();
            }
        }else{
            System.out.println("Usage: ClientDriver keyspace transactionFileName databaseServerIP ");
        }
    }

    private ClientDriver(String database, String fileName, String nodeIP) {
        this.cluster = Cluster.builder().addContactPoint(nodeIP).build();
        this.session = cluster.connect(database );
        this.database = database;
        this.fileName = fileName;

        this.deliveryObj = new Delivery(this.session);
        this.newOrderObj = new NewOrder(this.session);
        this.orderStatusObj = new OrderStatus(this.session);
        this.paymentObj = new Payment(this.session);
        this.popularItemObj = new PopularItem(this.session);
        this.stockLevelObj = new StockLevel(this.session);
        this.topBalanceObj = new TopBalance(this.session);
    }

    private void readTransactions() {
        int numItems;
        long startTime, endTime;
        String line, xactType;
        String[] paraList, newOrderParaList;
        String filePath = fileName;
        System.out.println(filePath);
        System.out.println("new");
        try {
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            line = br.readLine();
            int count = 0;
            while (line !=null){
                count++;
                System.out.println("Dealing with trans "+count);
                // split by comma, remove whitespaces around items
                paraList = line.trim().replaceAll("\\s","").split(",");
                //System.out.println(Arrays.toString(paraList));
                xactType = paraList[0].toUpperCase();
                //System.out.println(paraList[0]);
                switch(xactType.charAt(0)){
                    case 'N':
                        startTime = System.nanoTime();
                        // 5 paras
                        newOrderParaList = paraList.clone();
                        numItems = Integer.parseInt(paraList[4]);

                        String[][] itemsInfo = new String[numItems][];
                        for (int i=0; i<numItems; i++){
                            line = br.readLine().trim();
                            itemsInfo[i] = line.replaceAll("\\s","").split(",");
                        }
                        processNewOrder(newOrderParaList[1],newOrderParaList[2],newOrderParaList[3], numItems, itemsInfo);
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                    case 'P':
                        startTime = System.nanoTime();
                        // 5 paras
                        processPayment(paraList[1], paraList[2], paraList[3], paraList[4]);
                        endTime = System.nanoTime();
                        totalTime+= (endTime-startTime);
                        break;
                    case 'D':
                        startTime = System.nanoTime();
                        // 3 paras
                        processDelivery(paraList[1], paraList[2]);
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                    case 'O':
                        startTime = System.nanoTime();
                        // 4 paras
                        processOrderStatus(paraList[1], paraList[2], paraList[3]);
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                    case 'S':
                        startTime = System.nanoTime();
                        // 5 paras
                        processStockLevel(paraList[1], paraList[2], paraList[3], paraList[4]);
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                    case 'I':
                        startTime = System.nanoTime();
                        // 4 paras
                        processPopularItem(paraList[1], paraList[2], paraList[3]);
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                    case 'T':
                        startTime = System.nanoTime();
                        // 1 para
                        processTopBalance();
                        endTime = System.nanoTime();
                        totalTime += (endTime-startTime);
                        break;
                }
                line = br.readLine();
            }
            System.err.println("Total number of transactions processed: " + String.valueOf(totalNumXact));
            double totalTimeSeconds = (double)totalTime/1000000000.0;
            System.err.println("Total elapsed time: " + String.valueOf(totalTimeSeconds) + " seconds");
            double throughput = totalNumXact/totalTimeSeconds;
            System.err.println("Transaction throughput: " + String.valueOf(throughput) + "/sec");
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
    }

    private void processNewOrder(String c_id, String w_id, String d_id, int numItems, String[][] itemsInfo){
        System.out.println("new order");
        System.out.println(c_id +", "+ w_id +", " + d_id);
        System.out.println(Arrays.deepToString(itemsInfo));
        System.out.println("\n\n");
        newOrderObj.processNewOrder(Integer.parseInt(w_id), Integer.parseInt(d_id), Integer.parseInt(c_id), numItems, itemsInfo);
        totalNumXact += 1;
    }

    private void processPayment(String c_w_id, String c_d_id, String c_id, String payment){
        System.out.println("payment");
        System.out.println(c_w_id +", "+ c_d_id +", " + c_id + ", "+ payment);
        System.out.println("\n\n");
        paymentObj.processPaymentMade(Integer.parseInt(c_w_id), Integer.parseInt(c_d_id), Integer.parseInt(c_id), Double.parseDouble(payment));
        totalNumXact += 1;
    }

    private void processDelivery(String w_id, String carrier_id){
        System.out.println("delivery");
        System.out.println(w_id +", "+ carrier_id);
        System.out.println("\n\n");
        deliveryObj.processOldestDelivery(Integer.parseInt(w_id), Integer.parseInt(carrier_id));
        totalNumXact += 1;
    }

    private void processOrderStatus(String c_w_id, String c_d_id, String c_id){
        System.out.println("order status");
        System.out.println(c_w_id +", "+ c_d_id +", " + c_id);
        System.out.println("\n\n");
        orderStatusObj.getLastOrderStatus(Integer.parseInt(c_w_id), Integer.parseInt(c_d_id), Integer.parseInt(c_id));
        totalNumXact += 1;
    }

    private void processStockLevel(String w_id, String d_id, String t, String l){
        System.out.println("stock level");
        System.out.println(w_id +", "+ d_id +", " + t + ", "+ l);
        System.out.println("\n\n");
        stockLevelObj.checkStockLevel(Integer.parseInt(w_id), Integer.parseInt(d_id), Integer.parseInt(t), Integer.parseInt(l));
        totalNumXact += 1;
    }

    private void processPopularItem(String w_id, String d_id, String l){
        System.out.println("popular item");
        System.out.println(w_id +", "+ d_id +", " + l);
        System.out.println("\n\n");
        popularItemObj.findMostPopularItems(Integer.parseInt(w_id), Integer.parseInt(d_id), Integer.parseInt(l));
        totalNumXact += 1;
    }

    private void processTopBalance(){
        System.out.println("top balance");
        System.out.println("\n\n");
        topBalanceObj.findTopBalanceCustomers();
        totalNumXact += 1;
    }
}
