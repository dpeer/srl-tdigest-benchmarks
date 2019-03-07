package SrlTDigestBenchmarks.TransDigest;

import Common.LgData;
import Common.LgDimensionData;
import Common.SrlConsts;
import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class TransResAgg {
    private List<LgData> lgsData;
    private Map<String, List<ByteBuffer>> lgsTransTDigestsBuffers;
    private Set<String> transNames;
    private int threadsNum;

    TransResAgg(List<LgData> lgsData, int numTransactions, int threadsNum) {
        this.lgsData = lgsData;
        this.lgsTransTDigestsBuffers = new HashMap<>(numTransactions);
        this.transNames = new HashSet<>(numTransactions);
        this.threadsNum = threadsNum;
    }

    void createTDigests() throws InterruptedException {
        long totalStartDur, totalEndDur, startDur, endDur;
        int numTransactionsActual;

        System.out.println("TransResAgg.createTDigests: Merging " + lgsData.size() + " TDigests and converting to buffers");
        totalStartDur = System.currentTimeMillis();

        System.out.println("TransResAgg.createTDigests: Creating a list of transaction names");
        startDur = System.currentTimeMillis();
        // Create all trans names
        for (var lgData : lgsData) {
            transNames.addAll(lgData.getTransNames());
        }
        endDur = System.currentTimeMillis();
        numTransactionsActual = transNames.size();
        System.out.println("TransResAgg.createTDigests: Creating a list of transaction names duration (msec) = " + (endDur - startDur) + "; #Transactions = " + numTransactionsActual);

        System.out.println("ResAgg.createTDigests: Creating a map of transaction buffers");
        startDur = System.currentTimeMillis();
        var bufferNum = 0;
        // Create a map of transaction buffers
        for (var transName : transNames) {
            lgsTransTDigestsBuffers.put(transName, new ArrayList<>());
        }
        for (var lgData : lgsData) {
            for (var transTDigestsBufferEntry : lgData.getTransTDigestsBuffers().entrySet()) {
                lgsTransTDigestsBuffers.get(transTDigestsBufferEntry.getKey()).add(transTDigestsBufferEntry.getValue());
                bufferNum++;
            }
        }
        endDur = System.currentTimeMillis();
        System.out.println("ResAgg.createTDigests: Creating a map of transaction buffers duration (msec) = " + (endDur - startDur) +  "; #buffers = " + bufferNum);

        System.out.println("TransResAgg.createTDigests: Creating an empty tdigest per transaction");
        startDur = System.currentTimeMillis();
        // Create agg tdigest for each dimension
        var aggTDigests = new ConcurrentHashMap<String, MergingDigest>(numTransactionsActual);
        var aggByteBuffers = new ConcurrentHashMap<String, ByteBuffer>(numTransactionsActual);
        for (var transName : transNames) {
            aggTDigests.put(transName, new MergingDigest(SrlConsts.TdAggCompression));
        }
        endDur = System.currentTimeMillis();
        System.out.println("TransResAgg.createTDigests: Creating an empty tdigest per transaction duration (msec) = " + (endDur - startDur));

        System.out.println("TransResAgg.createTDigests: Group data for MT");
        startDur = System.currentTimeMillis();
        ConcurrentMap<Integer, List<Map.Entry<String, List<ByteBuffer>>>> threadsGroups = lgsTransTDigestsBuffers.entrySet()
                .stream()
                .collect(Collectors.groupingByConcurrent(lgsTransTDigestsBuffer -> Math.abs(lgsTransTDigestsBuffer.getKey().hashCode() % threadsNum)));
        endDur = System.currentTimeMillis();
        System.out.println("TransResAgg.createTDigests: Group data for MT duration (msec) = " + (endDur - startDur) + "; #Groups = " + threadsGroups.size());

        var threadsCount = threadsGroups.size();
        var threads = new ArrayList<TransAggTDigest>(threadsCount);
        for (var threadGroupKey : threadsGroups.keySet()) {
            threads.add(new TransAggTDigest("Thread " + threadGroupKey, aggTDigests, threadsGroups.get(threadGroupKey), aggByteBuffers));
        }
        for (var thread : threads) {
            thread.start();
        }

        for (var thread : threads) {
            thread.join();
        }

        totalEndDur = System.currentTimeMillis();
        System.out.println("TransResAgg.createTDigests: total duration (msec) = " + ((totalEndDur - totalStartDur)));

        var sumSize = 0;
        for (var aggByteBuffer : aggByteBuffers.values()) {
            sumSize += aggByteBuffer.position();
        }
        System.out.println("TransResAgg.createTDigests: total buffers size = " + sumSize);
    }
}
