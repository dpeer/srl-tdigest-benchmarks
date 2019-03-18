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
        var dbHost = System.getenv("TD_DB_HOST");
        if (dbHost == null) { dbHost = "localhost"; }
        var dbName = System.getenv("TD_DB_NAME");
        if (dbName == null) { dbName = "postgres"; }
        var dbUser = System.getenv("TD_DB_USER");
        if (dbUser == null) { dbUser = "postgres"; }
        var dbPass = System.getenv("TD_DB_PASS");
        if (dbPass == null) { dbPass = ""; }
        isInit = true;

        String url = "jdbc:postgresql://" + dbHost + ":5432/" + dbName;
        Properties props = new Properties();
        props.setProperty("user", dbUser);
        props.setProperty("password" ,dbPass);

        dbConnection = DriverManager.getConnection(url, props);
        return true;
    }

    public static Connection getDbConnection() {
        return dbConnection;
    }
}