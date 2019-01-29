package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import java.nio.ByteBuffer;
import java.util.Random;

class TDigestAgg extends Thread {
    private Thread t;
    private String threadName;
    private MergingDigest[] aggregatorTDigests;
    private ByteBuffer[][] lgsBuffers;
    private ByteBuffer[] aggsByteBuffers;
    private int start;
    private int end;

    TDigestAgg(String name, MergingDigest[] aggregatorTDigests, ByteBuffer[][] lgsBuffers, ByteBuffer[] aggsByteBuffers, int start, int end) {
        threadName = name;
        this.aggregatorTDigests = aggregatorTDigests;
        this.lgsBuffers = lgsBuffers;
        this.aggsByteBuffers = aggsByteBuffers;
        this.start = start;
        this.end = end;

        for (int i = this.start; i < this.end; i++) {
            this.aggregatorTDigests[i] = new MergingDigest(SrlConsts.TdAggCompression);
        }
    }

    public void run() {
        for (int i = start; i < end; i++) {
            for (int j = 0; j < SrlConsts.MaxDimensionsPerLg; j++) {
                aggregatorTDigests[((i % SrlConsts.MaxRegions) * SrlConsts.MaxDimensionsPerLg) + j].add(MergingDigest.fromBytes(lgsBuffers[i][j]));
            }
        }

        for (int i = start; i < end; i++) {
            int bufSize = aggregatorTDigests[i].smallByteSize();
            aggsByteBuffers[i] = ByteBuffer.allocate(bufSize);
            aggregatorTDigests[i].asSmallBytes(aggsByteBuffers[i]);
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

public class LgPerformanceBench {
    private static Random gen = new Random();
    private static long startDur;
    private static long endDur;
    private static final int threadsCount = 50;

    public static double[][] generateLgData() {
        // Create random data
        System.out.println("Generating " + SrlConsts.MaxDimensionsPerLg * SrlConsts.LgValuesPerDimension + " random values grouped by " + SrlConsts.MaxDimensionsPerLg + " dimensions per LGs");
        var data = new double[SrlConsts.MaxDimensionsPerLg][SrlConsts.LgValuesPerDimension];

        for (int i = 0; i < SrlConsts.MaxDimensionsPerLg; i++) {
            for (int j = 0; j< SrlConsts.LgValuesPerDimension; j++){
                data[i][j] = gen.nextDouble() * 100;
            }
        }

        return data;
    }

    public static ByteBuffer[] createLgDigestsBuffers(double[][] data) {
        System.out.println("createLgDigestsBuffers: Creating TDigests and converting to buffers for " + SrlConsts.MaxDimensionsPerLg + " dimensions with " + SrlConsts.LgValuesPerDimension + " values per dimension");
        startDur = System.currentTimeMillis();
        int bufSize;
        var lgsTDigests = new MergingDigest[SrlConsts.MaxDimensionsPerLg];
        var lgsByteBuffers = new ByteBuffer[SrlConsts.MaxDimensionsPerLg];

        for (int i = 0; i < SrlConsts.MaxDimensionsPerLg; i++) {
            lgsTDigests[i] = new MergingDigest(SrlConsts.TdCompression);
            for (int j = 0; j < SrlConsts.LgValuesPerDimension; j++) {
                lgsTDigests[i].add(data[i][j]);
            }
            bufSize = lgsTDigests[i].smallByteSize();
            lgsByteBuffers[i] = ByteBuffer.allocate(bufSize);
            lgsTDigests[i].asSmallBytes(lgsByteBuffers[i]);
        }
        endDur = System.currentTimeMillis();
        System.out.println("createLgDigestsBuffers: duration (msec) = " + ((endDur - startDur)));

        return lgsByteBuffers;
    }

    public static ByteBuffer[] mergeLgsTDigests(ByteBuffer[][] lgsBuffers) {
        System.out.println("mergeLgsTDigests: Merging " + lgsBuffers.length + " TDigests and converting to buffers ");
        startDur = System.currentTimeMillis();
        int bufSize;
        var aggregatorTDigests = new MergingDigest[SrlConsts.MaxDimensions];
        var aggsByteBuffers = new ByteBuffer[SrlConsts.MaxDimensions];
        for (int i = 0; i < SrlConsts.MaxDimensions; i++) {
            aggregatorTDigests[i] = new MergingDigest(SrlConsts.TdAggCompression);
        }

        for (int i = 0; i < SrlConsts.TDigestsPerResultsAggregator; i++) {
            for (int j = 0; j < SrlConsts.MaxDimensionsPerLg; j++) {
                aggregatorTDigests[((i % SrlConsts.MaxRegions) * SrlConsts.MaxDimensionsPerLg) + j].add(MergingDigest.fromBytes(lgsBuffers[i][j]));
            }
        }
        endDur = System.currentTimeMillis();
        System.out.println("mergeLgsTDigests: Add duration (msec) = " + ((endDur - startDur)));

        startDur = System.currentTimeMillis();
        for (int i = 0; i < SrlConsts.MaxDimensions; i++) {
            bufSize = aggregatorTDigests[i].smallByteSize();
            aggsByteBuffers[i] = ByteBuffer.allocate(bufSize);
            aggregatorTDigests[i].asSmallBytes(aggsByteBuffers[i]);
        }
        endDur = System.currentTimeMillis();
        System.out.println("mergeLgsTDigests: Convert to buffer duration (msec) = " + ((endDur - startDur)));


        return aggsByteBuffers;
    }

    public static ByteBuffer[] mergeLgsTDigestsMultiT(ByteBuffer[][] lgsBuffers) throws InterruptedException {
        System.out.println("mergeLgsTDigestsMultiT: Merging " + lgsBuffers.length + " TDigests and converting to buffers ");
        startDur = System.currentTimeMillis();

        var aggregatorTDigests = new MergingDigest[SrlConsts.MaxDimensions];
        var aggsByteBuffers = new ByteBuffer[SrlConsts.MaxDimensions];
        var threads = new Thread[threadsCount];

        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new TDigestAgg("Thread " + i, aggregatorTDigests, lgsBuffers, aggsByteBuffers, SrlConsts.MaxDimensions / threadsCount * i, SrlConsts.MaxDimensions / threadsCount * (i + 1));
            threads[i].start();
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].join();
        }

        endDur = System.currentTimeMillis();
        System.out.println("mergeLgsTDigestsMultiT: duration (msec) = " + ((endDur - startDur)));

        return aggsByteBuffers;
    }

    public static void main(String[] args) {

//        var data = generateLgData();
//        var lgsBuffers = createLgDigestsBuffers(data);


        var lgsBuffers = new ByteBuffer[SrlConsts.TDigestsPerResultsAggregator][];

        // Per LG:
        // 1. Generate random data
        // 2. Run TDigest
        // 3. Convert to ByteBuffer
        for (int i = 0; i < (SrlConsts.TDigestsPerResultsAggregator); i++) {
            System.out.println("LG #" + (i + 1) + "/" + SrlConsts.TDigestsPerResultsAggregator);
            double[][] data = generateLgData();
            lgsBuffers[i] = createLgDigestsBuffers(data);
        }

        // Reset buffers
        for (int i = 0; i < (SrlConsts.TDigestsPerResultsAggregator); i++) {
            for (int j = 0; j < SrlConsts.MaxDimensionsPerLg; j++) {
                lgsBuffers[i][j].position(0);
            }
        }

        mergeLgsTDigests(lgsBuffers);

        try {
            mergeLgsTDigestsMultiT(lgsBuffers);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // todo: Add processing of all Aggs and reporter
    }
}
