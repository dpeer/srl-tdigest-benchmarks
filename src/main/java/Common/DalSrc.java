package Common;

import Common.Pojos.RawDataPojo;
import Common.Pojos.TransDimensionPojo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DalSrc {

    private static String createTableName(String testId, String runId, String tableName) {
        return tableName + "_" + testId + "_" + runId;
    }

    public static List<TransDimensionPojo> getDimensions(String testId, String runId) throws SQLException {
        var pojos = new ArrayList<TransDimensionPojo>();
        var conn = DbConnSrc.getDbConnection();
        Statement st = conn.createStatement();

        // make sure autocommit is off
        conn.setAutoCommit(false);

        // Turn use of the cursor on.
        st.setFetchSize(100);
        ResultSet rs = st.executeQuery("SELECT id, script_id, transaction_name FROM " + createTableName(testId, runId, "transaction_dimensions") +
                " WHERE id IN (SELECT DISTINCT dimension_id FROM " + createTableName(testId, runId, "raw_trn_metrics_new") + ")");

        while (rs.next()) {
            pojos.add(new TransDimensionPojo(rs));
        }
        rs.close();

        // Turn the cursor off.
        st.setFetchSize(0);

        // Close the statement.
        st.close();

        return pojos;
    }

    public static List<RawDataPojo> getRawData(String testId, String runId, long from, long to) throws SQLException {
        var pojos = new ArrayList<RawDataPojo>();
        var conn = DbConnSrc.getDbConnection();
        var idx = 1;
        var tableName = "raw_trn_metrics_new";

        // make sure autocommit is off
        conn.setAutoCommit(false);
        var ps = conn.prepareStatement("SELECT dimension_id, duration FROM " + createTableName(testId, runId, tableName) +
                " WHERE start_time >= ? AND end_time < ? " +
                " AND transaction_status = 1");
        ps.setLong(idx++, from);
        ps.setLong(idx, to);

        // Turn use of the cursor on.
        ps.setFetchSize(5000);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            pojos.add(new RawDataPojo(rs));
        }
        rs.close();

        // Turn the cursor off.
        ps.setFetchSize(0);

        // Close the statement.
        ps.close();

        return pojos;
    }
}
