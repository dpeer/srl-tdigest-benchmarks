package SrlTDigestBenchmarks.DimensionDigest;

import Common.LgDimensionData;
import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class TDigestAgg extends Thread {
    private String threadName;
    private Map<String, MergingDigest> aggregatorTDigests;
    private List<LgDimensionData> lgsDimensionData;
    private Map<String, ByteBuffer> aggByteBuffers;
    private Set<String> dimensionsIds;

    TDigestAgg(String name, Map<String, MergingDigest> aggregatorTDigests, List<LgDimensionData> lgsDimensionData, Map<String, ByteBuffer> aggByteBuffers) {
        threadName = name;
        this.aggregatorTDigests = aggregatorTDigests;
        this.lgsDimensionData = lgsDimensionData;
        this.aggByteBuffers = aggByteBuffers;
        this.dimensionsIds = new HashSet<>();
    }

    public void run() {
        String lgDimensionId;
        for (var lgDimensionData : lgsDimensionData) {
            lgDimensionId = lgDimensionData.getDimensionId();
            aggregatorTDigests.get(lgDimensionId).add(MergingDigest.fromBytes(lgDimensionData.getTDigestBuffer()));
            dimensionsIds.add(lgDimensionId);
        }

        for (var dimensionId : dimensionsIds) {
            var aggTdigest = aggregatorTDigests.get(dimensionId);
            int bufSize = aggTdigest.smallByteSize();
            var byteBuffer = ByteBuffer.allocate(bufSize);
            aggTdigest.asSmallBytes(byteBuffer);
            aggByteBuffers.put(dimensionId, byteBuffer);
        }

        System.out.println(threadName + " exiting.");
    }
}
