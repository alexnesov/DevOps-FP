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

        String formattedURL = "jdbc:mysql://"+db_url;
        System.out.println(formattedURL);

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;

        try
            {
                // 1. Get a connection to database
                connection  = DriverManager.getConnection(formattedURL, db_user, db_pass);
                // 2. Create a statement
                stmt = connection.createStatement();
                System.out.println("Connected!");
                // 3. Execute SQL query
                rs = stmt.executeQuery("SELECT * FROM marketdata.sp500");

                // 4. Process the result set
            }
        catch(Exception ex)
            {
                System.out.println("Exception :" + ex.getMessage());
            }
        finally
            {
                if (rs != null)
                    {
                        rs.close();
                    }
                if (stmt != null)
                    {
                        stmt.close();
                    }
                if (connection != null)
                    {
                        connection.close();
                        System.out.println("Connection Closed.");
                    }
            }
    }
}

