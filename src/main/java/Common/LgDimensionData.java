package Common;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;

public class LgDimensionData {
    private final int region;
    private final int emulationId;
    private final int transId;
    private final String dimensionId;
    private double[] rawData;
    private ByteBuffer tDigestBuffer;
    private int bufSize = 0;

    public LgDimensionData(int region, int emulationId, int transId, double[] rawData) {
        this.region = region;
        this.emulationId = emulationId;
        this.transId = transId;
        this.rawData = rawData;
        this.dimensionId = region + "-" + emulationId + "-" + transId;
    }

    public void createTDigest() {
        var tDigest = new MergingDigest(SrlConsts.TdCompression);

        for (int i = 0; i < SrlConsts.LgValuesPerDimension; i++) {
            tDigest.add(rawData[i]);
        }

        bufSize = tDigest.smallByteSize();
        tDigestBuffer = ByteBuffer.allocate(bufSize);
        tDigest.asSmallBytes(tDigestBuffer);

        disposeRawData();
    }

    public void addToTDigest(TDigest tDigest) {
        for (int i = 0; i < SrlConsts.LgValuesPerDimension; i++) {
            tDigest.add(rawData[i]);
        }
    }

    public void disposeRawData() {
        rawData = null;
    }

    public void rewindBuffer() {
        tDigestBuffer.position(0);
    }

    public ByteBuffer getTDigestBuffer() {
        return tDigestBuffer;
    }

    public int getEmulationId() {
        return emulationId;
    }

    public int getTransId() {
        return transId;
    }

    public String getTransName() { return String.valueOf(transId); }

    public int getBufSize() {
        return bufSize;
    }

    public int getRegion() {
        return region;
    }

    public String getDimensionId() {
        return dimensionId;
    }
}
