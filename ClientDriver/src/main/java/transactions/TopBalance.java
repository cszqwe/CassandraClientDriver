package transactions;

import com.datastax.driver.core.*;
import java.util.ArrayList;

public class TopBalance {
    private PreparedStatement queryTopBalanceCust;
    private PreparedStatement queryGetMax,queryCustomer;
    Session session;
    public TopBalance(Session session){
        this.session = session;
        this.queryTopBalanceCust = session.prepare("select * from balance where id = 1 order by c_balance desc limit 10;");
        this.queryGetMax = session.prepare("select max(C_BALANCE) as C_BALANCE from customer ;");
        this.queryCustomer = session.prepare("select * from customer where c_w_id = ? and c_d_id = ? and c_id = ?;");
    }

    // client driver will all this function
    public void findTopBalanceCustomers() {
        BoundStatement boundMaxBalance = queryTopBalanceCust.bind();
        ResultSet result = session.execute(boundMaxBalance);
        while (!result.isExhausted()){
            Row resultRow = result.one();
            BoundStatement cus = queryCustomer.bind(resultRow.getInt("c_w_id"),resultRow.getInt("c_d_id"),resultRow.getInt("c_id"));
            ResultSet result2 = session.execute(cus);
            Row resultRow2 = result2.one();
            
            System.out.println("customer name - first: " + resultRow2.getString("C_FIRST"));
            System.out.println("customer name - middle: " + resultRow2.getString("C_MIDDLE"));
            System.out.println("customer name - last: " + resultRow2.getString("C_LAST"));
            System.out.println("customer balance: " + resultRow2.getDouble("C_BALANCE"));
            System.out.println("warehouse name of customer: " + resultRow2.getString("W_NAME"));
            System.out.println("district name of customer: " + resultRow2.getString("D_NAME"));
                    
        }

    }
}
