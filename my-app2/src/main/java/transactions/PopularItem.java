package transactions;


import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.*;
public class PopularItem {
    private PreparedStatement queryNextAvailOrderNum, queryOrderNumbers, queryPopularOrderLine, queryItemName;
    Session session;
    public PopularItem(Session session) {
        this.session = session;
        this.queryNextAvailOrderNum = session.prepare("select D_NEXT_O_ID from district where D_W_ID=? and D_ID=?;");
        this.queryOrderNumbers = session.prepare("select O_ID, O_ENTRY_D from orders where O_W_ID=? and O_D_ID=? and O_ID < ? and O_ID > ? ;");
        this.queryPopularOrderLine = session.prepare("select max(OL_QUANTITY)as OL_QUANTITY, OL_I_ID from order_line where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?;");
        this.queryItemName = session.prepare("select I_NAME from item where I_ID=?;");
    }

    // client driver will all this function
    public void findMostPopularItems(int w_id, int d_id, int numLastOrders) {
        int orderNum = getNextAvailOrderNum(w_id, d_id);
        findPopularItemXact(w_id, d_id, orderNum, numLastOrders);
    }

    private int getNextAvailOrderNum(int w_id, int d_id) {
        BoundStatement boundQueryNextAvail = queryNextAvailOrderNum.bind(w_id, d_id);
        ResultSet result = session.execute(boundQueryNextAvail);
        Row resultRow = result.one();

        int num = -1;
        if (resultRow != null) {
            num = resultRow.getInt("D_NEXT_O_ID");
        }
        return num;
    }

    private void findPopularItemXact(int w_id, int d_id, int orderNum, int numLastOrders) {
        BoundStatement boundQueryOrderNums = queryOrderNumbers.bind(w_id, d_id, orderNum, orderNum - numLastOrders);
        ResultSet result = session.execute(boundQueryOrderNums);

        ArrayList<Integer> orderNumList = new ArrayList<Integer>();
        ArrayList<Date> entryDateList = new ArrayList<Date>();
        if (result != null) {
            while (!result.isExhausted()) {
                Row rowResult = result.one();
                orderNumList.add(rowResult.getInt("O_ID"));
                entryDateList.add(rowResult.getTimestamp("O_ENTRY_D"));
            }
        }

        System.out.println("district identifier: W_ID->" + String.valueOf(w_id) + " D_ID->" + String.valueOf(d_id));
        System.out.println("number of last orders to be examined: " + String.valueOf(numLastOrders));

        // loop through each order
        for (int i=0; i<orderNumList.size(); i++) {
            System.out.println("order number: " + String.valueOf(orderNumList.get(i)) + "   entry date and time: " + entryDateList.get(i));

            BoundStatement boundQueryPopularOrderLine = queryPopularOrderLine.bind(w_id, d_id, orderNumList.get(i));
            result = session.execute(boundQueryPopularOrderLine);

            double maxQty  = 0;
            ArrayList<Integer> lst = new ArrayList<Integer>();
            if (result != null) {
                while (!result.isExhausted()) {
                    Row rowResult = result.one();
                    lst.add(rowResult.getInt("OL_I_ID"));
                    maxQty = rowResult.getDouble("OL_QUANTITY");
                }
            }

            for (int j=0; i<lst.size(); i++) {
                BoundStatement boundQueryItemName = queryItemName.bind(lst.get(j));
                result = session.execute(boundQueryItemName);
                Row rowResult = result.one();
                if (rowResult != null) {
                    System.out.println("popular item name: " + rowResult.getString("I_NAME") + "   quantity: " + String.valueOf(maxQty));
                }
            }
        }
    }
}
