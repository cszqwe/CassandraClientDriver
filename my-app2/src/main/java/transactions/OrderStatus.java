package transactions;

import com.datastax.driver.core.*;


public class OrderStatus {

    private PreparedStatement queryCustomerInfo, queryLastOrder, queryOrderItems;
    private Session session;
    public OrderStatus(Session session){
        this.session = session;
        // init cluster and session
        this.queryCustomerInfo = session.prepare("select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from customer where C_W_ID=? and C_D_ID=? and C_ID=?;");
        //this.queryLastOrder = session.prepare("select min(O_ID) as O_ID, O_ENTRY_D, O_CARRIER_ID from orders where O_W_ID=? and O_D_ID=? and O_C_ID=? ;");
        this.queryLastOrder = session.prepare("select max(O_ENTRY_D) as O_ENTRY_D,O_ID,O_CARRIER_ID from orders where o_w_id=? and o_d_id=? and o_c_id =?;");
        this.queryOrderItems = session.prepare("select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from order_line where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?;");
    }

    // client driver will all this function
    public void getLastOrderStatus(int c_w_id, int c_d_id, int c_id){
        getCustomerInfo(c_w_id, c_d_id, c_id);
        int lastOrderNum = getLastOrder(c_w_id, c_d_id, c_id);
        getLastOrderItemsInfo(c_w_id, c_d_id, lastOrderNum);
    }

    private void getCustomerInfo(int c_w_id, int c_d_id, int c_id){

        BoundStatement boundCustInfo = queryCustomerInfo.bind(c_w_id, c_d_id, c_id);
        ResultSet result = session.execute(boundCustInfo);
        Row resultRow = result.one();
        if (resultRow != null) {
            // output info
            System.out.println("customer name - first: " + resultRow.getString("C_FIRST"));
            System.out.println("customer name - middle: " + resultRow.getString("C_MIDDLE"));
            System.out.println("customer name - last: " + resultRow.getString("C_LAST"));
            System.out.println("customer balance: " + resultRow.getDouble("C_BALANCE"));
        }
    }

    private int getLastOrder(int c_w_id, int c_d_id, int c_id) {
        BoundStatement boundLastOrder = queryLastOrder.bind(c_w_id, c_d_id, c_id);
        ResultSet result = session.execute(boundLastOrder);
        Row resultRow = result.one();
        if (resultRow != null) {
            System.out.println("order number: " + resultRow.getInt("O_ID"));
            System.out.println("entry dat and time: " + resultRow.getTimestamp("O_ENTRY_D"));
            System.out.println("carrier identifier: " + resultRow.getInt("O_CARRIER_ID"));
            return resultRow.getInt("O_ID");
        }
        return -1;
    }

    private void getLastOrderItemsInfo(int c_w_id, int c_d_id, int lastOrderNum) {
        BoundStatement boundItemInfo = queryOrderItems.bind(c_w_id, c_d_id, lastOrderNum);
        ResultSet result = session.execute(boundItemInfo);

        if (result != null) {
            while (!result.isExhausted()) {
                Row resultRow = result.one();
                System.out.println("item number: " + resultRow.getInt("OL_I_ID"));
                System.out.println("supplying warehouse number: " + resultRow.getInt("OL_SUPPLY_W_ID"));
                System.out.println("quantity ordered: " + resultRow.getDouble("OL_QUANTITY"));
                System.out.println("total price: " + resultRow.getDouble("OL_AMOUNT"));
                System.out.println("date and time of delivery: " + resultRow.getTimestamp("OL_DELIVERY_D") + "\n");
            }
        }
    }
}
