package Common.Pojos;

import java.sql.ResultSet;
import java.sql.SQLException;

/*
dimension_id uuid,
duration double
*/
public class RawDataPojo {
    private String dimensionId;
    private double duration;

    public RawDataPojo(String dimensionId, double duration) {
        this.dimensionId = dimensionId;
        this.duration = duration;
    }

    public RawDataPojo(ResultSet rs) throws SQLException {
        int idx = 1;
        this.dimensionId = rs.getString(idx++);
        this.duration = rs.getDouble(idx);
    }

    public String getDimensionId() {
        return dimensionId;
    }

    public void setDimensionId(String dimensionId) {
        this.dimensionId = dimensionId;
    }

    public double getDuration() {
        return duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }
}
