package transactions;


import com.datastax.driver.core.*;
import java.util.ArrayList;


public class StockLevel {
    private PreparedStatement queryNextAvailOrderNum, queryItemNumbers, queryItemStock;
    Session session;
    public StockLevel(Session session){
        this.session = session;
        this.queryNextAvailOrderNum = session.prepare("select D_NEXT_O_ID from district where D_W_ID=? and D_ID=?;");
        this.queryItemNumbers = session.prepare("select OL_I_ID from order_line where Ol_W_ID=? and Ol_D_ID=? and OL_O_ID < ? and OL_O_ID >= ?;");
        this.queryItemStock = session.prepare("select s_qty from stocks where S_W_ID=? and S_I_ID=?;");
    }

    // client driver will all this function
    public void checkStockLevel(int w_id, int d_id, int threshold, int numLastOrders) {
        int orderNum = getNextAvailOrderNum(w_id, d_id);
        findLowStockItems(w_id, d_id, orderNum, numLastOrders, threshold);
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

    private void findLowStockItems(int w_id, int d_id, int orderNum, int numLastOrders, int threshold) {
        BoundStatement boundQueryItemNums = queryItemNumbers.bind(w_id, d_id, orderNum, orderNum-numLastOrders);
        ResultSet result = session.execute(boundQueryItemNums);

        if (result!=null) {
            ArrayList<Integer> itemNumList = new ArrayList<Integer>();
            while (!result.isExhausted()) {
                Row rowResult = result.one();
                itemNumList.add(rowResult.getInt("OL_I_ID"));
            }

            int count = 0;
            for (int i=0; i<itemNumList.size(); i++) {
                BoundStatement boundItemStock = queryItemStock.bind(w_id, itemNumList.get(i));
                result = session.execute(boundItemStock);
                Row rowResult = result.one();
                if (rowResult!=null && rowResult.getInt("S_QTY") < threshold) {
                    count ++;
                }
            }
            System.out.println("There are " + String.valueOf(count) + " items where its stock quantity at W_ID=" + String.valueOf(w_id) + " is below the threshold " + String.valueOf(threshold));
        }
    }
}
