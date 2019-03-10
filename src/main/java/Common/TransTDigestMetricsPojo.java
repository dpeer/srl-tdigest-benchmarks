package Common;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*
load_test_run_id text,
start_time bigint,
end_time bigint,
script_id text,
transaction_name text,
grouped boolean,
tdigest_buf bytea
*/
public class TransTDigestMetricsPojo {
    private String loadTestRunId;
    private long startTime;
    private long endTime;
    private String scriptId;
    private String transactionId;
    private boolean grouped;
    private byte[] tdigestBuf;

    public TransTDigestMetricsPojo(String loadTestRunId, long startTime, long endTime, String scriptId, String transactionId, boolean grouped, byte[] tdigestBuf) {
        this.loadTestRunId = loadTestRunId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.scriptId = scriptId;
        this.transactionId = transactionId;
        this.grouped = grouped;
        this.tdigestBuf = tdigestBuf;
    }

    public TransTDigestMetricsPojo(ResultSet rs) throws SQLException {
        int idx = 1;
        this.loadTestRunId = rs.getString(idx++);
        this.startTime = rs.getLong(idx++);
        this.endTime = rs.getLong(idx++);
        this.scriptId = rs.getString(idx++);
        this.transactionId = rs.getString(idx++);
        this.grouped = rs.getBoolean(idx++);
        this.tdigestBuf = rs.getBytes(idx);
    }

    public void updatePreparedStatement(PreparedStatement ps) throws SQLException {
        int idx = 1;
        ps.setString(idx++, loadTestRunId);
        ps.setLong(idx++, startTime);
        ps.setLong(idx++, endTime);
        ps.setString(idx++, scriptId);
        ps.setString(idx++, transactionId);
        ps.setBoolean(idx++, grouped);
        ps.setBytes(idx, tdigestBuf);
    }

    public String getLoadTestRunId() {
        return loadTestRunId;
    }

    public void setLoadTestRunId(String loadTestRunId) {
        this.loadTestRunId = loadTestRunId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getScriptId() {
        return scriptId;
    }

    public void setScriptId(String scriptId) {
        this.scriptId = scriptId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public boolean isGrouped() {
        return grouped;
    }

    public void setGrouped(boolean grouped) {
        this.grouped = grouped;
    }

    public byte[] getTdigestBuf() {
        return tdigestBuf;
    }

    public void setTdigestBuf(byte[] tdigestBuf) {
        this.tdigestBuf = tdigestBuf;
    }
}
