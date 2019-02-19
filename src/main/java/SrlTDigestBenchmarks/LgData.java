package SrlTDigestBenchmarks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

class LgData {
    private ArrayList<LgDimensionData> lgDimensionsData;
    private int region;
    private static Random gen = new Random();

    LgData(int region, double[][] rawData) {
        this.region = region;
        lgDimensionsData = new ArrayList<>(SrlConsts.MaxDimensionsPerLg);

        for (int i = 0; i < SrlConsts.MaxEmulations; i++) {
            var rand = gen.nextInt();
            for (int j = 0; j < SrlConsts.MaxTransactionsPerLgPerInterval; j++) {
                lgDimensionsData.add(new LgDimensionData(
                        region,
                        i,
                        j + (rand % 2 == 0 ? 0 : SrlConsts.MaxTransactionsPerLgPerInterval),
                        rawData[i * SrlConsts.MaxTransactionsPerLgPerInterval + j])
                );
            }
        }
    }

    LgData(int region, double[][] rawData, int numOfEmulations, int numOfTransactions) {
        this.region = region;
        lgDimensionsData = new ArrayList<>(numOfEmulations * numOfTransactions);

        for (int i = 0; i < numOfEmulations; i++) {
            for (int j = 0; j < numOfTransactions; j++) {
                lgDimensionsData.add(new LgDimensionData(region, i, j, rawData[i * numOfTransactions + j]));
            }
        }
    }

    void createTDigests() {
//        System.out.println("createTDigests: Creating TDigests and converting to buffers for " + SrlConsts.MaxDimensionsPerLg + " dimensions with " + SrlConsts.LgValuesPerDimension + " values per dimension");
//        var startDur = System.currentTimeMillis();

        for (var lgDimensionData : lgDimensionsData) {
            lgDimensionData.createTDigest();
        }

//        var endDur = System.currentTimeMillis();
//        System.out.println("createTDigests: duration (msec) = " + ((endDur - startDur)));
    }

    void rewindBuffers() {
        for (var lgDimensionData : lgDimensionsData) {
            lgDimensionData.rewindBuffer();
        }
    }

    Set<String> getDimensionsIds() {
        var dimensionsIds = new HashSet<String>();
        for (var lgDimensionData : lgDimensionsData) {
            dimensionsIds.add(lgDimensionData.getDimensionId());
        }
        return dimensionsIds;
    }

    int getRegion() {
        return region;
    }

    ArrayList<LgDimensionData> getLgDimensionsData() {
        return lgDimensionsData;
    }
}