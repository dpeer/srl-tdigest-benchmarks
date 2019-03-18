package Common.Pojos;

import java.sql.ResultSet;
import java.sql.SQLException;

/*
id uuid,
script_id text,
transaction_name text
*/
public class TransDimensionPojo {
    private String id;
    private String scriptId;
    private String transactionName;

    public TransDimensionPojo(String id, String scriptId, String transactionName) {
        this.id = id;
        this.scriptId = scriptId;
        this.transactionName = transactionName;
    }

    public TransDimensionPojo(ResultSet rs) throws SQLException {
        int idx = 1;
        this.id = rs.getString(idx++);
        this.scriptId = rs.getString(idx++);
        this.transactionName = rs.getString(idx);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getScriptId() {
        return scriptId;
    }

    public void setScriptId(String scriptId) {
        this.scriptId = scriptId;
    }

    public String getTransactionName() {
        return transactionName;
    }

    public void setTransactionName(String transactionName) {
        this.transactionName = transactionName;
    }
}
