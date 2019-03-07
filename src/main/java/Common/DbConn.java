package Common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbConn
{
    private boolean isInit;
    private static Connection dbConnection;

    private DbConn() {
        isInit = false;
        dbConnection = null;
    }

    // Inner class to provide instance of class
    private static class DbConnSingleton {
        private static final DbConn INSTANCE = new DbConn();
    }

    public static DbConn getInstance() {
        return DbConnSingleton.INSTANCE;
    }

    public boolean isInit() {
        return isInit;
    }

    public boolean init(String url, Properties props) throws SQLException {
        if (isInit) {
            return false;
        }
        dbConnection = DriverManager.getConnection(url, props);
        isInit = true;
        return true;
    }

    public Connection getDbConnection() {
        return dbConnection;
    }
}