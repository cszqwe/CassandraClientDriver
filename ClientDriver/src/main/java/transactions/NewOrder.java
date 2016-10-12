package transactions;


import com.datastax.driver.core.*;

import java.sql.Timestamp;
import java.util.Date;

public class NewOrder {
    private PreparedStatement queryNextAvailOrderNum, updateNextAvailOrderNum, queryCreateNewOrder, queryStockQuantity, updateStock, queryItemPriceAndName, queryCreateOrderLine, queryCustomerDiscountAndTax;
    private double totalAmount;
    private Session session;
    public NewOrder(Session session){
        this.session = session;
        this.totalAmount = 0.0;

        this.queryNextAvailOrderNum = session.prepare("select D_NEXT_O_ID from district where D_W_ID=? and D_ID=?;");
        this.updateNextAvailOrderNum = session.prepare("update district set D_NEXT_O_ID=? where D_W_ID=? and D_ID=?;");

        this.queryCreateNewOrder = session.prepare("insert into orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT) values(?,?,?,?,?,?,?);");

        this.queryStockQuantity = session.prepare("select S_QTY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT from stocks where S_W_ID=? and S_I_ID=?;");
        this.updateStock = session.prepare("update stocks set S_QTY=?, S_YTD=?, S_ORDER_CNT=?, S_REMOTE_CNT=? where S_W_ID=? and S_I_ID=?;");

        this.queryItemPriceAndName = session.prepare("select I_PRICE, I_NAME from item where I_ID=?");
        this.queryCreateOrderLine = session.prepare("insert into order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY) values(?,?,?,?,?,?,?,?,?);");

        this.queryCustomerDiscountAndTax = session.prepare("select C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX from customer where C_W_ID=? and C_D_ID=? and C_ID=?");
    }

    // client driver will all this function
    public void processNewOrder(int w_id, int d_id, int c_id, int totalNumItems, String[][] itemInfo){
        // size of itemInfo = totalNumItems
        // eg: first item:(convert to int) itemNumber = [0][0], supplierWarehouse=[0][1], quantity=[0][2]

        int orderNum = getAndUpdateNextAvailOrderNum(w_id, d_id);
        createNewOrder(w_id, d_id, orderNum, c_id, totalNumItems);

        // for each item in the order, do the following
        for (int i=0; i<itemInfo.length; i++) {
            int itemNum = Integer.parseInt(itemInfo[i][0]);
            int supplierWarehouse = Integer.parseInt(itemInfo[i][1]);
            int quantity = Integer.parseInt(itemInfo[i][2]);

            int newStock = updateStock(supplierWarehouse, itemNum, quantity, w_id);
            createOrderLine(w_id, itemNum, quantity, supplierWarehouse, d_id, orderNum, i+1, newStock);
        }
        calculateTotalAmount(w_id, d_id, c_id, totalNumItems);
    }

    private int getAndUpdateNextAvailOrderNum(int w_id, int d_id){
        BoundStatement boundQueryOrderNum = queryNextAvailOrderNum.bind(w_id, d_id);
        ResultSet result = session.execute(boundQueryOrderNum);
        Row resultRow = result.one();

        int num=-1;
        if (resultRow != null){
            num = resultRow.getInt("D_NEXT_O_ID");

            BoundStatement boundUpdateOrderNum = updateNextAvailOrderNum.bind(num+1, w_id, d_id);
            session.execute(boundUpdateOrderNum);
        }
        return num;
    }

    private void createNewOrder(int w_id, int d_id, int orderNum, int c_id, int totalNumItems){
        Date today = new Date();
        Timestamp currentTimeStamp = new Timestamp(today.getTime());
        BoundStatement boundNewOrder = queryCreateNewOrder.bind(w_id, d_id, orderNum, c_id, currentTimeStamp, -1, totalNumItems);
        session.execute(boundNewOrder);

        System.out.println("order number: " + String.valueOf(orderNum));
        System.out.println("entry date: " + currentTimeStamp.toString());
    }

    private int updateStock(int supplierWarehouse, int itemNum, int quantity, int w_id) {
        BoundStatement boundStockQty = queryStockQuantity.bind(supplierWarehouse, itemNum);
        ResultSet result = session.execute(boundStockQty);
        Row resultRow = result.one();

        int adjustedQty = -1;
        if (resultRow != null){
            adjustedQty =resultRow.getInt("S_QTY") - quantity;
            if (adjustedQty < 10) {
                adjustedQty += 100;
            }
            double stockYtd = resultRow.getDouble("S_YTD") + quantity;
            int stockOrderCnt = resultRow.getInt("S_ORDER_CNT") + 1;
            int stockRemoteCnt = resultRow.getInt("S_REMOTE_CNT");
            if (stockRemoteCnt != w_id) {
                stockRemoteCnt += 1;
            }
            BoundStatement boundUpdateStock = updateStock.bind(adjustedQty, stockYtd, stockOrderCnt, stockRemoteCnt, w_id, itemNum);
            session.execute(boundUpdateStock);
        }
        return adjustedQty;
    }

    private void createOrderLine(int w_id, int itemNum, int quantity, int supplierWarehouse, int d_id, int orderNum, int ol_number, int newStock){
        BoundStatement boundGetPriceAndName = queryItemPriceAndName.bind(itemNum);
        ResultSet result = session.execute(boundGetPriceAndName);
        Row resultRow = result.one();

        if (resultRow != null) {
            double itemAmt = resultRow.getDouble("I_PRICE") * quantity;
            totalAmount += itemAmt;
            String itemName = resultRow.getString("I_NAME");
            //this.queryCreateOrderLine = session.prepare("insert into order_line (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY) values(?,?,?,?,?,?,?,?,?);");

            BoundStatement boundCreateOrderLine = queryCreateOrderLine.bind(w_id, d_id, orderNum, ol_number, null, itemAmt,supplierWarehouse, itemNum, (double)quantity);
            session.execute(boundCreateOrderLine);

            System.out.println("item number: " + String.valueOf(itemNum));
            System.out.println("item name: " + String.valueOf(itemName));
            System.out.println("item supplier warehouse: " + String.valueOf(supplierWarehouse));
            System.out.println("item quantity: " + String.valueOf(quantity));
            System.out.println("item amount: " + String.valueOf(itemAmt));
            System.out.println("item stock: "+ String.valueOf(newStock));
        }
    }

    private void calculateTotalAmount(int w_id, int d_id, int c_id, int totalNumItems) {
        BoundStatement boundQueryCustInfo = queryCustomerDiscountAndTax.bind(w_id, d_id, c_id);
        ResultSet result = session.execute(boundQueryCustInfo);
        Row resultRow = result.one();

        if (resultRow != null) {

            double discount = resultRow.getDouble("C_DISCOUNT");
            double wTax = resultRow.getDouble("W_TAX");
            double dTax = resultRow.getDouble("D_TAX");

            totalAmount = totalAmount * (1 + dTax + wTax) * (1 - discount);
            System.out.println("customer identifier: W_ID->" + String.valueOf(w_id) + ", D_ID->" + String.valueOf(d_id) + ", C_ID->" + String.valueOf(c_id));
            System.out.println("customer last name: " + resultRow.getString("C_LAST"));
            System.out.println("customer credit: " + resultRow.getString("C_CREDIT"));
            System.out.println("customer discount: " + String.valueOf(discount));
            System.out.println("warehouse tax rate: " + String.valueOf(wTax));
            System.out.println("district tax rate: " + String.valueOf(dTax));
            System.out.print("number of items: " + String.valueOf(totalNumItems));
            System.out.println("total amount for order: " + String.valueOf(totalAmount));
        }
    }
}
