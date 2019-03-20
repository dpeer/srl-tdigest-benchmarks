package SrlTDigestBenchmarks.TransDigest;

import Common.Config;
import Common.Dal;
import Common.LgData;
import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

class TransResAgg {
    private long startTime;
    private long endTime;
    private List<LgData> lgsData;
    private Map<String, List<ByteBuffer>> lgsTransTDigestsBuffers;
    private Set<String> transNames;
    private int threadsNum;

    TransResAgg(long startTime, long endTime, List<LgData> lgsData, int numTransactions, int threadsNum) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.lgsData = lgsData;
        this.lgsTransTDigestsBuffers = new HashMap<>(numTransactions);
        this.transNames = new HashSet<>(numTransactions);
        this.threadsNum = threadsNum;
    }

    void createTDigests(boolean saveToDB) {
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
            aggTDigests.put(transName, new MergingDigest(Config.getInstance().getTdAggCompression()));
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
        var threads = new ArrayList<TransAggTDigestThread>(threadsCount);
        for (var threadGroupKey : threadsGroups.keySet()) {
            threads.add(new TransAggTDigestThread("Thread " + threadGroupKey, aggTDigests, threadsGroups.get(threadGroupKey), aggByteBuffers));
        }

        threads.forEach(Thread::start);

        threads.forEach(thread-> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        totalEndDur = System.currentTimeMillis();
        System.out.println("TransResAgg.createTDigests: total duration (msec) = " + ((totalEndDur - totalStartDur)));

        var sumSize = 0;
        for (var aggByteBuffer : aggByteBuffers.values()) {
            sumSize += aggByteBuffer.position();
        }
        System.out.println("TransResAgg.createTDigests: total buffers size = " + sumSize);

        if (saveToDB) {
            System.out.println("TransResAgg.createTDigests: Saving to DB");
            startDur = System.currentTimeMillis();

            try {
                Dal.saveTDigestTransMetrics(startTime, endTime, aggByteBuffers);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            endDur = System.currentTimeMillis();
            System.out.println("TransResAgg.createTDigests: Saving to DB duration (msec) = " + (endDur - startDur));
        }
    }
}
