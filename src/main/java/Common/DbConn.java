package Common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbConn {
    private static boolean isInit = false;
    private static Connection dbConnection = null;

    public static boolean isInit() {
        return isInit;
    }

    public static boolean init() throws SQLException {
        if (isInit) {
            return true;
        }
        isInit = true;

        String url = "jdbc:postgresql://localhost:5432/postgres";
        Properties props = new Properties();
        props.setProperty("user","postgres");

        dbConnection = DriverManager.getConnection(url, props);
        return true;
    }

    public static Connection getDbConnection() {
        return dbConnection;
    }
}