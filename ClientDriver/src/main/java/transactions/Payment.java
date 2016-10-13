package transactions;

import com.datastax.driver.core.*;

public class Payment {
    private PreparedStatement queryDeleteBalance,queryUpdateBalance,queryDistrictYTD, queryWarehouseInfo, queryWarehouseYtd, queryUpdateWare, queryDistrictInfo, queryDistrictYtd, queryUpdateDist, queryCustomerInfo, queryUpdateCust;
    Session session;
    public Payment(Session session){
        // init cluster and session

        // init prepared statements
        this.session = session;
        this.queryWarehouseInfo = session.prepare("select W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from warehouse_unchanged where W_ID =?;");
        this.queryWarehouseYtd = session.prepare("select W_YTD from warehouse where W_ID=?;");
        this.queryUpdateWare = session.prepare("update warehouse set W_YTD=? where W_ID=? if W_YTD=?;");
        this.queryDistrictYtd = session.prepare("select D_YTD from district where D_W_ID=? and D_ID=?;");
        this.queryDistrictInfo = session.prepare("select D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from district_unchanged where D_W_ID=? and D_ID=?;");
        this.queryUpdateDist = session.prepare("update district set D_YTD=? where D_W_ID=? and D_ID=? if D_YTD=?;");

        this.queryCustomerInfo = session.prepare("select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT from customer where C_W_ID=? and C_D_ID=? and C_ID=?;");
        this.queryUpdateCust = session.prepare("update customer set C_BALANCE=?, C_YTD_PAYMENT=?, C_PAYMENT_CNT=? where C_W_ID=? and C_D_ID=? and C_ID=? if C_PAYMENT_CNT = ?;");
        this.queryDeleteBalance = session.prepare("delete from balance where id=? and c_balance = ? and C_W_ID=? and C_D_ID=? and C_ID=?;");
        this.queryUpdateBalance = session.prepare("insert into balance (id, c_balance, c_w_id, c_d_id, c_id) values(?,?,?,?,?);");

    }

    // client driver will all this function
    public void processPaymentMade(int c_w_id, int c_d_id, int c_id, double paymentAmount){
        double currentWarehouseYtd = getWarehouseInfo(c_w_id);
        incrementWarehouseYtd(c_w_id, paymentAmount, currentWarehouseYtd);

        double currentDistrictYtd = getDistrictInfo(c_w_id, c_d_id);
        incrementDistrictYtd(c_w_id, c_d_id, paymentAmount, currentDistrictYtd);

        Double[] arr = getCustomerInfo(c_w_id, c_d_id, c_id);
        updateCustomerInfo(c_w_id, c_d_id, c_id, paymentAmount, arr[0], arr[1], arr[2].intValue());
    }

    // select warehouse address info and current YTD amount, return current YTD
    private double getWarehouseInfo(int c_w_id){
        // get address info, output
        BoundStatement boundWareInfo = queryWarehouseInfo.bind(c_w_id);
        ResultSet result = session.execute(boundWareInfo);
        Row resultRow = result.one();
        if (resultRow != null) {
            // output info
            System.out.println("warehouse address - Street 1: " + resultRow.getString("W_STREET_1"));
            System.out.println("warehouse address - Street 2: " + resultRow.getString("W_STREET_2"));
            System.out.println("warehouse address - city: " + resultRow.getString("W_CITY"));
            System.out.println("warehouse address - state: " + resultRow.getString("W_ZIP"));
        }

        // get current YTD amount
        BoundStatement boundWareYtd = queryWarehouseYtd.bind(c_w_id);
        result = session.execute(boundWareYtd);
        resultRow = result.one();
        if (resultRow != null) {
            return resultRow.getDouble("W_YTD");
        }
        return -1.0;
    }

    // increment W_YTD in warehouse by paymentAmount
    private void incrementWarehouseYtd(int c_w_id, double paymentAmount, double currentAmt){
        Double newYtdValue = currentAmt + paymentAmount;
        BoundStatement boundUpdateWare = queryUpdateWare.bind(newYtdValue, c_w_id, currentAmt);
        session.execute(boundUpdateWare);
    }

    // select district address info and current YTD amount, return current YTD
    private double getDistrictInfo(int c_w_id, int c_d_id){

        // get address info, output
        BoundStatement boundDistInfo = queryDistrictInfo.bind(c_w_id, c_d_id);
        ResultSet result = session.execute(boundDistInfo);
        Row resultRow = result.one();
        if (resultRow != null){
            // output info
            System.out.println("district address - Street 1: " + resultRow.getString("D_STREET_1"));
            System.out.println("district address - Street 2: " + resultRow.getString("D_STREET_2"));
            System.out.println("district address - city: " + resultRow.getString("D_CITY"));
            System.out.println("district address - state: " + resultRow.getString("D_ZIP"));
        }

        // get current ytd amount
        System.out.println("debug "+ c_w_id+" "+c_d_id);
        BoundStatement boundDistYtd = queryDistrictYtd.bind(c_w_id, c_d_id);
        result = session.execute(boundDistYtd);
        resultRow = result.one();
        if (resultRow != null){

            return resultRow.getDouble("d_ytd");
        }
        return -1.0;
    }

    // increment D_YTD in district by paymentAmount
    private void incrementDistrictYtd(int c_w_id, int c_d_id, double paymentAmount, double currentAmt){
        Double newYtdValue = currentAmt + paymentAmount;
        BoundStatement boundUpdateDist = queryUpdateDist.bind(newYtdValue, c_w_id, c_d_id, currentAmt);
        session.execute(boundUpdateDist);
    }

    // select customer name, address, phone, since, credit , credit_limit, discount
    // get balance(double), ytd(double), cnt(int)-->(return these three values)
    private Double[] getCustomerInfo(int c_w_id, int c_d_id, int c_id) {
        Double[] values = new Double[3];

        BoundStatement boundCustInfo = queryCustomerInfo.bind(c_w_id, c_d_id, c_id);
        ResultSet result = session.execute(boundCustInfo);
        Row resultRow = result.one();
        if (resultRow != null){
            // output info
            System.out.println("customer address - Street 1: " + resultRow.getString("C_STREET_1"));
            System.out.println("customer address - Street 2: " + resultRow.getString("C_STREET_2"));
            System.out.println("customer address - city: " + resultRow.getString("C_CITY"));
            System.out.println("customer address - state: " + resultRow.getString("C_ZIP"));
            System.out.println("customer name - first: " + resultRow.getString("C_FIRST"));
            System.out.println("customer name - middle: " + resultRow.getString("C_MIDDLE"));
            System.out.println("customer name - last: " + resultRow.getString("C_LAST"));
            System.out.println("customer phone: " + resultRow.getString("C_PHONE"));
            System.out.println("customer since: " + resultRow.getTimestamp("C_SINCE"));
            System.out.println("customer credit: " + resultRow.getString("C_CREDIT"));
            System.out.println("customer credit limit: " + resultRow.getDouble("C_CREDIT_LIM"));
            System.out.println("customer discount: " + resultRow.getDouble("C_DISCOUNT"));

            values[0] = resultRow.getDouble("C_BALANCE");
            values[1] = resultRow.getDouble("C_YTD_PAYMENT");
            values[2] = (double)resultRow.getInt("C_PAYMENT_CNT");
        }
        return values;
    }

    private void updateCustomerInfo(int c_w_id, int c_d_id, int c_id, double paymentAmount, double balance, double currentYtdAmount, int paymentCount) {
        Double newBalance = balance - paymentAmount;
        Double newYtd = currentYtdAmount + paymentAmount;
        int newCount = paymentCount + 1;
        // decrement C_BALANCE in customer by paymentAmount
        // increment C_YTD_PAYMENT in customer by paymentAmount
        // increment C_PAYMENT_CNT in customer by 1
        BoundStatement boundUpdateCust = queryUpdateCust.bind(newBalance, newYtd, newCount, c_w_id, c_d_id, c_id, paymentCount);
        session.execute(boundUpdateCust);
        BoundStatement boundDeleteBalance = queryDeleteBalance.bind(1, balance, c_w_id, c_d_id, c_id);
        session.execute(boundDeleteBalance);
        BoundStatement boundUpdateBalance = queryUpdateBalance.bind(1,newBalance, c_w_id, c_d_id, c_id);
        session.execute(boundUpdateBalance);

    }
}
