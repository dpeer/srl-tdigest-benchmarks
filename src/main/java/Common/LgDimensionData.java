package Common;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;

public class LgDimensionData {
    private final int region;
    private final int emulationId;
    private final String transName;
    private final String dimensionId;
    private double[] rawData;
    private ByteBuffer tDigestBuffer;
    private int bufSize = 0;

    public LgDimensionData(int region, int emulationId, String transName, double[] rawData) {
        this.region = region;
        this.emulationId = emulationId;
        this.transName = transName;
        this.rawData = rawData;
        this.dimensionId = region + "-" + emulationId + "-" + transName;
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
        for (int i = 0; i < rawData.length; i++) {
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

    public String getTransName() {
        return transName;
    }

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
