package SrlTDigestBenchmarks;

import java.util.ArrayList;
import java.util.Random;

//class DimensionId {
//    private int region;
//    private int emulationId;
//    private int trxId;
//
//    DimensionId(int region, int emulationId, int trxId) {
//        this.region = region;
//        this.emulationId = emulationId;
//        this.trxId = trxId;
//    }
//
//    int getEmulationId() {
//        return emulationId;
//    }
//
//    int getTrxId() {
//        return trxId;
//    }
//
//    int getRegion() {
//        return region;
//    }
//}

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

    int getRegion() {
        return region;
    }

    ArrayList<LgDimensionData> getLgDimensionsData() {
        return lgDimensionsData;
    }
}