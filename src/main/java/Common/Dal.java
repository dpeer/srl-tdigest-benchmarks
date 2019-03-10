package Common;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Dal {

    public static List<TransTDigestMetricsPojo> getTDigestTransMetrics() throws SQLException {
        var pojos = new ArrayList<TransTDigestMetricsPojo>();
        var conn = DbConn.getDbConnection();

        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement st = conn.createStatement();

        // Turn use of the cursor on.
        st.setFetchSize(50);
        ResultSet rs = st.executeQuery("SELECT * FROM tdigest_transaction_metrics");
        while (rs.next()) {
            pojos.add(new TransTDigestMetricsPojo(rs));
        }
        rs.close();

        // Turn the cursor off.
        st.setFetchSize(0);

        // Close the statement.
        st.close();

        return pojos;
    }

    public static void saveTDigestTransMetrics(String loadTestId, long startTime, long endTime, Map<String, ByteBuffer> tdigestsBuffers) throws SQLException {
        var conn = DbConn.getDbConnection();
        var ps = conn.prepareStatement("INSERT INTO tdigest_transaction_metrics VALUES (?, ?, ?, ?, ?, ?, ?)");

        for (var tdigest : tdigestsBuffers.entrySet()) {
            var pojo = new TransTDigestMetricsPojo(
                    loadTestId,
                    startTime,
                    endTime,
                    tdigest.getKey(),
                    tdigest.getKey(),
                    false,
                    tdigest.getValue().array()
            );
            pojo.updatePreparedStatement(ps);
            ps.executeUpdate();
        }

        ps.close();
    }

    //public static
}
