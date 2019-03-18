package Common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbConnSrc {
    private static boolean isInit = false;
    private static Connection dbConnection = null;

    public static boolean isInit() {
        return isInit;
    }

    public static boolean init() throws SQLException {
        if (isInit) {
            return true;
        }
        var dbHostSrc = System.getenv("TD_DB_HOST_SRC");
        var dbNameSrc = System.getenv("TD_DB_NAME_SRC");
        var dbUserSrc = System.getenv("TD_DB_USER_SRC");
        var dbPassSrc = System.getenv("TD_DB_PASS_SRC");
        if (dbHostSrc.length() == 0 || dbNameSrc.length() == 0 || dbUserSrc.length() == 0 || dbPassSrc.length() == 0) {
            return false;
        }
        isInit = true;

        String url = "jdbc:postgresql://" + dbHostSrc + ":5432/" + dbNameSrc;
        Properties props = new Properties();
        props.setProperty("user", dbUserSrc);
        props.setProperty("password" ,dbPassSrc);

        dbConnection = DriverManager.getConnection(url, props);
        return true;
    }

    public static Connection getDbConnection() {
        return dbConnection;
    }
}