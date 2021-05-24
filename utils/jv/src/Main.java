import java.sql.*;

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
                ResultSetMetaData rsmd = rs.getMetaData();

                // 4.a Get Number of cols, needed for .getColumnName() argument
                int nbCols = rsmd.getColumnCount();

                // 4b. Get the column names
                for (int i = 1; i <= nbCols; i++)
                    {
                        System.out.println(rsmd.getColumnName(i));
                    }
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

