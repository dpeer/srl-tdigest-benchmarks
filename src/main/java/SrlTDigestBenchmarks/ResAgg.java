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
    private ConcurrentHashMap<String, MergingDigest> aggregatorTDigests;
    private List<LgDimensionData> lgsDimensionData;
    private ConcurrentHashMap<String, ByteBuffer> aggsByteBuffers;
    private Set<String> dimensionsIds;

    TDigestAgg(String name, ConcurrentHashMap<String, MergingDigest> aggregatorTDigests, List<LgDimensionData> lgsDimensionData, ConcurrentHashMap<String, ByteBuffer> aggsByteBuffers) {
        threadName = name;
        this.aggregatorTDigests = aggregatorTDigests;
        this.lgsDimensionData = lgsDimensionData;
        this.aggsByteBuffers = aggsByteBuffers;
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
            aggsByteBuffers.put(dimensionId, byteBuffer);
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
    private ArrayList<LgDimensionData> lgsDimensionsData;
    private HashSet<String> dimensionsIds = new HashSet<>(SrlConsts.MaxDimensions);

    ResAgg(List<LgData> lgsData) {
        this.lgsData = lgsData;
    }

    void createTDigests() throws InterruptedException {
        long totalStartDur, totalEndDur, startDur, endDur;

        System.out.println("ResAgg.createTDigests: Merging " + lgsData.size() + " TDigests and converting to buffers");
        totalStartDur = System.currentTimeMillis();

        System.out.println("ResAgg.createTDigests: Creating a list of LGs dimensions");
        startDur = System.currentTimeMillis();
        // Create a list of LGs dimensions
        lgsDimensionsData = new ArrayList<>(SrlConsts.TDigestsPerResultsAggregator * SrlConsts.MaxDimensionsPerLg);
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
        System.out.println("ResAgg.createTDigests: Creating a list of dimensions ids duration (msec) = " + (endDur - startDur) + "; #Dimensions = " + dimensionsIds.size());

        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension");
        startDur = System.currentTimeMillis();
        // Create agg tdigest for each dimension
        var aggTDigests = new ConcurrentHashMap<String, MergingDigest>(SrlConsts.MaxDimensions);
        var aggsByteBuffers = new ConcurrentHashMap<String, ByteBuffer>(SrlConsts.MaxDimensions);
        for (var dimensionId : dimensionsIds) {
            aggTDigests.put(dimensionId, new MergingDigest(SrlConsts.TdAggCompression));
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension duration (msec) = " + (endDur - startDur));

        System.out.println("ResAgg.createTDigests: Group data for MT");
        startDur = System.currentTimeMillis();
        ConcurrentMap<Integer, List<LgDimensionData>> threadsGroups = lgsDimensionsData
                .stream()
                .collect(Collectors.groupingByConcurrent(lgDimensionData -> lgDimensionData.getEmulationId() * 1000 + lgDimensionData.getRegion()));
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Group data for MT duration (msec) = " + (endDur - startDur) + "; #Groups = " + threadsGroups.size());

        var threadsCount = threadsGroups.size();
        var threads = new ArrayList<TDigestAgg>(threadsCount);
        for (var threadGroupKey : threadsGroups.keySet()) {
            threads.add(new TDigestAgg("Thread " + threadGroupKey.toString(), aggTDigests, threadsGroups.get(threadGroupKey), aggsByteBuffers));
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
