package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;

class LgDimensionData {
    private final int region;
    private final int emulationId;
    private final int trxId;
    private final String dimensionId;
    private double[] rawData;
    private ByteBuffer tDigestBuffer;
    private int bufSize = 0;

    LgDimensionData(int region, int emulationId, int trxId, double[] rawData) {
        this.region = region;
        this.emulationId = emulationId;
        this.trxId = trxId;
        this.rawData = rawData;
        this.dimensionId = region + "-" + emulationId + "-" + trxId;
    }

    void createTDigest() {
        var tDigest = new MergingDigest(SrlConsts.TdCompression);

        for (int i = 0; i < SrlConsts.LgValuesPerDimension; i++) {
            tDigest.add(rawData[i]);
        }

        bufSize = tDigest.smallByteSize();
        tDigestBuffer = ByteBuffer.allocate(bufSize);
        tDigest.asSmallBytes(tDigestBuffer);

        rawData = null;
    }

    void rewindBuffer() {
        tDigestBuffer.position(0);
    }

    ByteBuffer getTDigestBuffer() {
        return tDigestBuffer;
    }

    int getEmulationId() {
        return emulationId;
    }

    int getTrxId() {
        return trxId;
    }

    int getBufSize() {
        return bufSize;
    }

    int getRegion() {
        return region;
    }

    String getDimensionId() {
        return dimensionId;
    }
}
