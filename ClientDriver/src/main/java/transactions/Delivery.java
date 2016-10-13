package transactions;

import com.datastax.driver.core.*;
import java.sql.Timestamp;
import java.util.Date;


public class Delivery {

    private PreparedStatement queryDeleteBalance,queryUpdateBalance,queryOldestOrderInfo, queryUpdateCarrier, queryUpdateDeliveryTime, queryUpdateCustomer, queryOrderLineAmount, queryBalanceAndCount;
    private Session session;
    public Delivery(Session session){
        // if order has not been delivered, change carrier_id from null to -1
        // return O_ID and C_ID
        this.session = session;
        this.queryOldestOrderInfo = session.prepare("select min(O_ID) as O_ID, O_C_ID, O_OL_CNT from orders where O_W_ID=? and O_D_ID=? and O_CARRIER_ID=-1;");
        this.queryUpdateCarrier = session.prepare("update orders set O_CARRIER_ID=? where O_W_ID=? and O_D_ID=? and O_ID=?;");

        this.queryOrderLineAmount = session.prepare("select OL_AMOUNT from order_line where OL_W_ID=? and OL_D_ID=? and OL_O_ID=? and OL_NUMBER=?;");
        this.queryUpdateDeliveryTime = session.prepare("update order_line set OL_DELIVERY_D=? where OL_W_ID=? and OL_D_ID=? and OL_O_ID=? and OL_NUMBER=?;");

        this.queryBalanceAndCount = session.prepare("select C_BALANCE, C_DELIVERY_CNT from customer where C_w_ID=? and C_D_ID=? and C_ID=?;");
        this.queryUpdateCustomer = session.prepare("update customer set C_BALANCE=?, C_DELIVERY_CNT=? where C_W_ID=? and C_D_ID=? and C_ID=?;");
        this.queryDeleteBalance = session.prepare("delete from balance where id = ? and c_balance = ? and C_W_ID=? and C_D_ID=? and C_ID=?;");
        this.queryUpdateBalance = session.prepare("insert into balance (id, c_balance, c_w_id, c_d_id, c_id) values(?,?,?,?,?);");
    }

    // client driver will all this functioncreateOrderTable
    public void processOldestDelivery(int w_id, int carrier_id){
        for (int district=1; district<=10; district++) {
            //return o_id and c_id
            int[] arr = handleOrderTable(w_id, district, carrier_id);
            int orderNum = arr[0];
            int customerId = arr[1];
            int countNum = arr[2];
            // return ol amount
            double amount = handleOrderLineTable(w_id, district, orderNum,countNum);
            handleCustomerTable(w_id, district, customerId, amount);
        }
    }

    private int[] handleOrderTable(int w_id, int d_id, int carrier_id){
        BoundStatement boundOldestOrder = queryOldestOrderInfo.bind(w_id, d_id);
        ResultSet result = session.execute(boundOldestOrder);
        Row resultRow = result.one();

        int[] temp = new int[3];
        if (resultRow != null){
            temp[0] = resultRow.getInt("O_ID");
            temp[1] = resultRow.getInt("O_C_ID");
            temp[2] = resultRow.getInt("O_OL_CNT");
            BoundStatement boundUpdateCarrier = queryUpdateCarrier.bind(carrier_id, w_id, d_id, temp[0]);
            session.execute(boundUpdateCarrier);
        }
        return temp;
    }

    private double handleOrderLineTable(int w_id, int d_id, int o_id,int countNum){

        double totalAmount = 0.0;
        for (int i = 1; i <= countNum; i++){
            BoundStatement boundAmount = queryOrderLineAmount.bind(w_id, d_id, o_id, i);
            ResultSet result = session.execute(boundAmount);

            if (result != null) {
                if (!result.isExhausted()) {
                    Row resultRow = result.one();
                    totalAmount += resultRow.getDouble("OL_AMOUNT");
                }
            }

            // update delivery time
            Date today = new Date();
            Timestamp currentTimeStamp = new Timestamp(today.getTime());
            BoundStatement boundUpdateTime = queryUpdateDeliveryTime.bind(currentTimeStamp, w_id, d_id, o_id, i);
            session.execute(boundUpdateTime);
        }
        return totalAmount;
    }

    private void handleCustomerTable(int w_id, int d_id, int c_id, double amount) {
        BoundStatement boundRetrieveCust = queryBalanceAndCount.bind(w_id, d_id, c_id);
        ResultSet result = session.execute(boundRetrieveCust);

        if (result!=null){
            Row resultRow =result.one();
            Double balance = resultRow.getDouble("C_BALANCE");
            int deliveryCount = resultRow.getInt("C_DELIVERY_CNT");

            BoundStatement boundUpdateCust = queryUpdateCustomer.bind(balance + amount, deliveryCount + 1, w_id, d_id, c_id);
            session.execute(boundUpdateCust);
            BoundStatement boundDeleteBalance = queryDeleteBalance.bind(1, balance, w_id, d_id, c_id);
            session.execute(boundDeleteBalance);
            BoundStatement boundUpdateBalance = queryUpdateBalance.bind(1, balance+ amount, w_id, d_id, c_id);
            session.execute(boundUpdateBalance);
                
            
        }

    }
}
