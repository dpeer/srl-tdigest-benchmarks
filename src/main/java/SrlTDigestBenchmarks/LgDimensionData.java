package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;

class LgDimensionData {
    private int region;
    private int emulationId;
    private int trxId;
    private double[] rawData;
    private ByteBuffer tDigestBuffer;
    private int bufSize = 0;

    LgDimensionData(int region, int emulationId, int trxId, double[] rawData) {
        this.region = region;
        this.emulationId = emulationId;
        this.trxId = trxId;
        this.rawData = rawData;
    }

    void createTDigest() {
        var tDigest = new MergingDigest(SrlConsts.TdCompression);

        for (int i = 0; i < SrlConsts.LgValuesPerDimension; i++) {
            tDigest.add(rawData[i]);
        }

        bufSize = tDigest.smallByteSize();
        tDigestBuffer = ByteBuffer.allocate(bufSize);
        tDigest.asSmallBytes(tDigestBuffer);
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
        return region + "-" + emulationId + "-" + trxId;
    }
}
