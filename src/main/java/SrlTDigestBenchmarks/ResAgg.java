package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class ResAgg {
    private List<LgData> lgsData;
    private List<LgDimensionData> lgsDimensionsData;
    private Set<String> dimensionsIds;
    private int numDimensions;
    private int threadsNum;

    ResAgg(List<LgData> lgsData, int numDimensions, int threadsNum) {
        this.lgsData = lgsData;
        this.numDimensions = numDimensions;
        this.dimensionsIds = new HashSet<>(numDimensions);
        this.threadsNum = threadsNum;
    }

    void createTDigests() throws InterruptedException {
        long totalStartDur, totalEndDur, startDur, endDur;
        int numDimensionsActual;

        System.out.println("ResAgg.createTDigests: Merging " + lgsData.size() + " TDigests and converting to buffers");
        totalStartDur = System.currentTimeMillis();

        System.out.println("ResAgg.createTDigests: Creating a list of LGs dimensions");
        startDur = System.currentTimeMillis();
        // Create a list of LGs dimensions data
        lgsDimensionsData = new ArrayList<>(SrlConsts.MaxLgs * numDimensions);
        for (var lgData : lgsData) {
            lgsDimensionsData.addAll(lgData.getLgDimensionsData());
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating a list of LGs dimensions data duration (msec) = " + (endDur - startDur) +  "; #lgsDimensionsData = " + lgsDimensionsData.size());

        System.out.println("ResAgg.createTDigests: Creating a list of dimensions ids");
        startDur = System.currentTimeMillis();
        // Create all dimensions ids
        for (var lgDimensionData : lgsDimensionsData) {
            dimensionsIds.add(lgDimensionData.getDimensionId());
        }
        endDur = System.currentTimeMillis();
        numDimensionsActual = dimensionsIds.size();
        System.out.println("ResAgg.createTDigests: Creating a list of dimensions ids duration (msec) = " + (endDur - startDur) + "; #Dimensions = " + numDimensionsActual);

        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension");
        startDur = System.currentTimeMillis();
        // Create agg tdigest for each dimension
        var aggTDigests = new ConcurrentHashMap<String, MergingDigest>(numDimensionsActual);
        var aggByteBuffers = new ConcurrentHashMap<String, ByteBuffer>(numDimensionsActual);
        for (var dimensionId : dimensionsIds) {
            aggTDigests.put(dimensionId, new MergingDigest(SrlConsts.TdAggCompression));
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating an empty tdigest per dimension duration (msec) = " + (endDur - startDur));

        System.out.println("ResAgg.createTDigests: Group data for MT");
        startDur = System.currentTimeMillis();
        ConcurrentMap<Integer, List<LgDimensionData>> threadsGroups = lgsDimensionsData
                .stream()
                .collect(Collectors.groupingByConcurrent(lgDimensionData -> Math.abs(lgDimensionData.getDimensionId().hashCode() % threadsNum)));
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

        var sumSize = 0;
        for (var aggByteBuffer : aggByteBuffers.values()) {
            sumSize += aggByteBuffer.position();
        }
        System.out.println("ResAgg.createTDigests: total buffers size = " + sumSize);
    }
}
