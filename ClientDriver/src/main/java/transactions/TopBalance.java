package transactions;

import com.datastax.driver.core.*;
import java.util.ArrayList;

public class TopBalance {
    private PreparedStatement queryTopBalanceCust;
    private PreparedStatement queryGetMax;
    Session session;
    public TopBalance(Session session){
        this.session = session;
        this.queryTopBalanceCust = session.prepare("select C_BALANCE, C_FIRST, C_MIDDLE, C_LAST,  W_NAME, D_NAME from customer where C_BALANCE = ?;");
        this.queryGetMax = session.prepare("select max(C_BALANCE) as C_BALANCE from customer ;");
    }

    // client driver will all this function
    public void findTopBalanceCustomers() {
        int remaining = 10;
        BoundStatement boundMaxBalance = queryGetMax.bind();
        ResultSet resultTmp = session.execute(boundMaxBalance);
        Row resultRowTmp = resultTmp.one();
        double max = resultRowTmp.getDouble("C_BALANCE");
        while (true){
            BoundStatement boundTopBalance = queryTopBalanceCust.bind(max);
            ResultSet result = session.execute(boundTopBalance);
            if (result != null) {
                while (!result.isExhausted()) {
                    Row resultRow = result.one();
                    // output info
                    System.out.println("customer name - first: " + resultRow.getString("C_FIRST"));
                    System.out.println("customer name - middle: " + resultRow.getString("C_MIDDLE"));
                    System.out.println("customer name - last: " + resultRow.getString("C_LAST"));
                    System.out.println("customer balance: " + resultRow.getDouble("C_BALANCE"));
                    System.out.println("warehouse name of customer: " + resultRow.getString("W_NAME"));
                    System.out.println("district name of customer: " + resultRow.getString("D_NAME"));
                    remaining--;
                    if (remaining == 0) return;
                    max = resultRow.getDouble("C_BALANCE");
                }
                
            }
            max -= 0.01;
        }

    }
}
