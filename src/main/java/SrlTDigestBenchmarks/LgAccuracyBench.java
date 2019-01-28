package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

public class LgAccuracyBench {
    public static void main(String[] args) {
        Random gen = new Random();
        long startDur;
        long endDur;
        final double percentile = 95;
        final int perSteps = 5;
        final double perDelta = 1;
        final int totalDataSize = SrlConsts.MaxLgs * SrlConsts.LgValuesPerDimension;

        var data = new double[totalDataSize];
        var lgsTDigests = new MergingDigest[SrlConsts.MaxLgs];
        var byteBuffers = new ByteBuffer[SrlConsts.MaxLgs];
        var fromLGsTDigests = new ArrayList<MergingDigest>(SrlConsts.MaxLgs);
        var javaPer = new Percentile();
        var pcsValues = new double[perSteps];
        var tdAllLgs = new MergingDigest(SrlConsts.TdAggCompression);
        var percentiles = SrlCommon.generatePercentileMarkers(percentile, perDelta, perSteps);

        // Create random data
        System.out.println("Generating " + totalDataSize + " random values grouped by " + SrlConsts.MaxLgs + " LGs");
        for (int i = 0; i < totalDataSize; i++) {
            data[i] = gen.nextDouble() * 100;
        }

        // Create LGs tds
        startDur = System.currentTimeMillis();
        for (int i = 0; i < SrlConsts.MaxLgs; i++) {
            lgsTDigests[i] = new MergingDigest(SrlConsts.TdCompression);
            for (int j = 0; j < SrlConsts.LgValuesPerDimension; j++) {
                lgsTDigests[i].add(data[(i * SrlConsts.LgValuesPerDimension) + j]);
            }
        }
        endDur = System.currentTimeMillis();
        System.out.println("TDigest LGs add duration (msec) = " + ((endDur - startDur)));

        // Convert sub tds to bytes
        startDur = System.currentTimeMillis();
        int bufSize = 0;
        for (int i = 0; i < SrlConsts.MaxLgs; i++) {
            bufSize = lgsTDigests[i].smallByteSize();
            byteBuffers[i] = ByteBuffer.allocate(bufSize);
            lgsTDigests[i].asSmallBytes(byteBuffers[i]);
        }
        endDur = System.currentTimeMillis();
        System.out.println("TDigest LGs convert to byte duration (msec) = " + ((endDur - startDur)));
        System.out.println("TDigest LGs buff size = " + bufSize);

        //////////////////////////////////////////////////////////////////////////////////////////

        // Convert bytes to sub tds
        startDur = System.currentTimeMillis();
        for (int i = 0; i < SrlConsts.MaxLgs; i++) {
            byteBuffers[i].position(0);
            fromLGsTDigests.add(MergingDigest.fromBytes(byteBuffers[i]));
        }
        tdAllLgs.add(fromLGsTDigests);
        endDur = System.currentTimeMillis();
        System.out.println("TDigest Subs convert from byte and merge duration (msec) = " + ((endDur - startDur)));


        startDur = System.currentTimeMillis();
        javaPer.setData(data);
        for (int i = 0; i < perSteps; i++) {
            pcsValues[i] = javaPer.evaluate(percentiles[i]);
        }
        endDur = System.currentTimeMillis();
        System.out.println("Percentiles calculation duration (msec) = " + ((endDur - startDur)));

        startDur = System.currentTimeMillis();
        var tAllLgsValue = tdAllLgs.quantile(percentile * 1e-2);
        endDur = System.currentTimeMillis();
        System.out.println("TDigest All LGs calculation duration (msec) = " + ((endDur - startDur)));

        System.out.println("TDigest All LGs = " + tAllLgsValue);

        for (int i = 0; i < perSteps; i++) {
            System.out.println("Percentile " + percentiles[i] + " = " + pcsValues[i]);
        }

        for (int i = 0; i < perSteps; i++) {
            if (i == 0) {
                if (tAllLgsValue < pcsValues[i]){
                    System.out.println("TDigest Subs is below percentile " + percentiles[i]);
                    break;
                } else if ((i + 1) < perSteps && tAllLgsValue >= pcsValues[i] && tAllLgsValue <= pcsValues[i + 1]) {
                    System.out.println("TDigest Subs is between percentiles " + percentiles[i] + " and " + percentiles[i + 1]);
                    break;
                }
            } else if (i == (perSteps-1)) {
                System.out.println("TDigest Subs is above percentile " + percentiles[i]);
            } else {
                if (tAllLgsValue >= pcsValues[i] && tAllLgsValue <= pcsValues[i + 1]) {
                    System.out.println("TDigest Subs is between percentiles " + percentiles[i] + " and " + percentiles[i + 1]);
                    break;
                }
            }
        }
    }}
