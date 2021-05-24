import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("MySQL connect example.");
        String db_pass = System.getenv("aws_db_pass");
        String db_user = System.getenv("aws_db_user");
        String db_url = System.getenv("aws_db_endpoint");

        System.out.println(db_url);

        Class.forName("com.mysql.cj.jdbc.Driver");


        try {
            // 1. Get a connection to database
            Connection connection  = DriverManager.getConnection("jdbc:mysql://flaskfinance.ccxri6cskobf.eu-central-1.rds.amazonaws.com", db_user, db_pass);

            // 2. Create a statement
            Statement stmt = connection.createStatement();
            System.out.println("Connected!");

            // 3. Execute SQL query
            ResultSet rs = stmt.executeQuery("SELECT * FROM marketdata.sp500");

            // 4. Process the result set
        } catch(Exception ex)
            {
                System.out.println("Exception :" + ex.getMessage());
            }
    }
}

