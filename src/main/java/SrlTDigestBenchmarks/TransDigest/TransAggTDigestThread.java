package SrlTDigestBenchmarks.TransDigest;

import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class TransAggTDigestThread extends Thread {
    private String threadName;
    private Map<String, MergingDigest> aggregatorTDigests;
    private List<Map.Entry<String, List<ByteBuffer>>> lgsTransTDigestsBuffersEntries;
    private Map<String, ByteBuffer> aggByteBuffers;
    private Set<String> transNames;

    TransAggTDigestThread(String name, Map<String, MergingDigest> aggregatorTDigests, List<Map.Entry<String, List<ByteBuffer>>> lgsTransTDigestsBuffersEntries, Map<String, ByteBuffer> aggByteBuffers) {
        threadName = name;
        this.aggregatorTDigests = aggregatorTDigests;
        this.lgsTransTDigestsBuffersEntries = lgsTransTDigestsBuffersEntries;
        this.aggByteBuffers = aggByteBuffers;
        this.transNames = new HashSet<>();
    }

    public void run() {
        for (var lgsTransTDigestsBufferEntry : lgsTransTDigestsBuffersEntries) {
            var transName = lgsTransTDigestsBufferEntry.getKey();
            var aggregatorTDigest = aggregatorTDigests.get(transName);
            for (var byteBuffer : lgsTransTDigestsBufferEntry.getValue()) {
                aggregatorTDigest.add(MergingDigest.fromBytes(byteBuffer));
            }
            transNames.add(transName);
        }

        for (var transName : transNames) {
            var aggTdigest = aggregatorTDigests.get(transName);
            int bufSize = aggTdigest.smallByteSize();
            var byteBuffer = ByteBuffer.allocate(bufSize);
            aggTdigest.asSmallBytes(byteBuffer);
            aggByteBuffers.put(transName, byteBuffer);
        }

        System.out.println(threadName + " exiting.");
    }
}
