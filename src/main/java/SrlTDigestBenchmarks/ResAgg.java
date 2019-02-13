package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class TDigestAgg extends Thread {
    private Thread t;
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
        for (var lgDimensionData : lgsDimensionData) {
            aggregatorTDigests.get(lgDimensionData.getDimensionId()).add(MergingDigest.fromBytes(lgDimensionData.getTDigestBuffer()));
            dimensionsIds.add(lgDimensionData.getDimensionId());
        }

        for (var dimensionId : dimensionsIds) {
            var aggTdigest = aggregatorTDigests.get(dimensionId);
            int bufSize = aggTdigest.smallByteSize();
            var byteBuffer = ByteBuffer.allocate(bufSize);
            aggTdigest.asSmallBytes(byteBuffer);
            aggByteBuffers.put(dimensionId, byteBuffer);
        }

        System.out.println("Thread " +  threadName + " exiting.");
    }

    public void start() {
        System.out.println("Starting " +  threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }
}

public class ResAgg {
    private List<LgData> lgsData;
    private List<LgDimensionData> lgsDimensionsData;
    private Set<String> dimensionsIds = new HashSet<>(SrlConsts.MaxDimensions);

    ResAgg(List<LgData> lgsData) {
        this.lgsData = lgsData;
    }

    void createTDigests() throws InterruptedException {
        long totalStartDur, totalEndDur, startDur, endDur;
        int numDimensions;

        System.out.println("ResAgg.createTDigests: Merging " + lgsData.size() + " TDigests and converting to buffers");
        totalStartDur = System.currentTimeMillis();

        System.out.println("ResAgg.createTDigests: Creating a list of LGs dimensions");
        startDur = System.currentTimeMillis();
        // Create a list of LGs dimensions
        lgsDimensionsData = new ArrayList<>(SrlConsts.MaxDimensions / SrlConsts.ResultsAggregatorsNum);
        for (var lgData : lgsData) {
            lgsDimensionsData.addAll(lgData.getLgDimensionsData());
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating a list of LGs dimensions duration (msec) = " + (endDur - startDur));

        System.out.println("ResAgg.createTDigests: Creating a list of dimensions ids");
        startDur = System.currentTimeMillis();
        // Create all dimensions ids
        for (var lgDimensionData : lgsDimensionsData) {
            dimensionsIds.add(lgDimensionData.getDimensionId());
        }
        endDur = System.currentTimeMillis();
        numDimensions = dimensionsIds.size();
        System.out.println("ResAgg.createTDigests: Creating a list of dimensions ids duration (msec) = " + (endDur - startDur) + "; #Dimensions = " + numDimensions);

        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension");
        startDur = System.currentTimeMillis();
        // Create agg tdigest for each dimension
        var aggTDigests = new ConcurrentHashMap<String, MergingDigest>(numDimensions);
        var aggByteBuffers = new ConcurrentHashMap<String, ByteBuffer>(numDimensions);
        for (var dimensionId : dimensionsIds) {
            aggTDigests.put(dimensionId, new MergingDigest(SrlConsts.TdAggCompression));
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension duration (msec) = " + (endDur - startDur));

        System.out.println("ResAgg.createTDigests: Group data for MT");
        startDur = System.currentTimeMillis();
        ConcurrentMap<String, List<LgDimensionData>> threadsGroups = lgsDimensionsData
                .stream()
                .collect(Collectors.groupingByConcurrent(lgDimensionData -> lgDimensionData.getEmulationId() + "-" + lgDimensionData.getRegion()));
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Group data for MT duration (msec) = " + (endDur - startDur) + "; #Groups = " + threadsGroups.size());

        var threadsCount = threadsGroups.size();
        var threads = new ArrayList<TDigestAgg>(threadsCount);
        for (var threadGroupKey : threadsGroups.keySet()) {
            threads.add(new TDigestAgg("Thread " + threadGroupKey, aggTDigests, threadsGroups.get(threadGroupKey), aggByteBuffers));
        }
        for (var thread : threads) {
            thread.start();
        }

        for (var thread : threads) {
            thread.join();
        }

        totalEndDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: total duration (msec) = " + ((totalEndDur - totalStartDur)));
    }
}
