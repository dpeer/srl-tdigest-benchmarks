package SrlTDigestBenchmarks.TransDigest;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

class TransTDigestMetricsPojo {
    private String loadTestRunId;
    private int startTime;
    private int endTime;
    private String scriptId;
    private String transactionName;
    private Boolean grouped;
    private ByteBuffer tdigestBuf;

    public TransTDigestMetricsPojo(ResultSet res) throws SQLException {
        var idx = 1;
        loadTestRunId = res.getString(idx++);
        startTime = res.getInt(idx++);
        endTime = res.getInt(idx++);
        scriptId = res.getString(idx++);
        transactionName = res.getString(idx++);
        grouped = res.getBoolean(idx++);
        //tdigestBuf = res.getBytes(idx++);
    }

    public String getLoadTestRunId() {
        return loadTestRunId;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public String getScriptId() {
        return scriptId;
    }

    public String getTransactionName() {
        return transactionName;
    }

    public Boolean getGrouped() {
        return grouped;
    }

    public ByteBuffer getTdigestBuf() {
        return tdigestBuf;
    }
}
