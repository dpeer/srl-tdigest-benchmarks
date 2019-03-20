package Common;

import Common.Pojos.RawDataPojo;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class LgData {
    private ArrayList<LgDimensionData> lgDimensionsData;
    private int region;
    private Map<String, TDigest> transTDigests;
    private Map<String, ByteBuffer> transTDigestsBuffers;
    private static Random gen = new Random();

    public LgData(int region, double[][] rawData) {
        this.region = region;
        lgDimensionsData = new ArrayList<>(Config.getInstance().getMaxDimensionsPerLg());

        for (int i = 0; i < Config.getInstance().getMaxEmulations(); i++) {
            var rand = gen.nextInt();
            for (int j = 0; j < Config.getInstance().getMaxTransactionsPerLgPerInterval(); j++) {
                lgDimensionsData.add(new LgDimensionData(
                        region,
                        i,
                        String.valueOf(j + (rand % 2 == 0 ? 0 : Config.getInstance().getMaxTransactionsPerLgPerInterval())),
                        rawData[i * Config.getInstance().getMaxTransactionsPerLgPerInterval() + j])
                );
            }
        }
    }

    public LgData(int region, double[][] rawData, int numOfEmulations, int numOfTransactions) {
        this.region = region;
        lgDimensionsData = new ArrayList<>(numOfEmulations * numOfTransactions);

        for (int i = 0; i < numOfEmulations; i++) {
            for (int j = 0; j < numOfTransactions; j++) {
                lgDimensionsData.add(new LgDimensionData(region, i, String.valueOf(j), rawData[i * numOfTransactions + j]));
            }
        }
    }

    public LgData(int region, List<RawDataPojo> rawData) {
        this.region = region;
        lgDimensionsData = new ArrayList<>();

        var rawDataByDimension = rawData.stream().collect(Collectors.groupingBy(RawDataPojo::getDimensionId));

        for (var rawDataByDimensionKey : rawDataByDimension.keySet()) {
            lgDimensionsData.add(new LgDimensionData(
                    region,
                    0,
                    rawDataByDimensionKey,
                    rawDataByDimension.get(rawDataByDimensionKey).stream().mapToDouble(RawDataPojo::getDuration).toArray())
            );
        }
    }

    public void createTDigests() {
//        System.out.println("createTDigests: Creating TDigests and converting to buffers for " + Config.getInstance().getMaxDimensionsPerLg() + " dimensions with " + Config.getInstance().getLgValuesPerDimension() + " values per dimension");
//        var startDur = System.currentTimeMillis();

        for (var lgDimensionData : lgDimensionsData) {
            lgDimensionData.createTDigest();
        }

//        var endDur = System.currentTimeMillis();
//        System.out.println("createTDigests: duration (msec) = " + ((endDur - startDur)));
    }

    public void createTransTDigests() {
        transTDigests = new HashMap<>();
        transTDigestsBuffers = new HashMap<>();
        var transNames = new HashSet<String>();
        for (var lgDimensionData : lgDimensionsData) {
            transNames.add(lgDimensionData.getTransName());
        }

        for (var transName : transNames) {
            transTDigests.put(transName, new MergingDigest(Config.getInstance().getTdCompression()));
        }

        for (var lgDimensionData : lgDimensionsData) {
            lgDimensionData.addToTDigest(transTDigests.get(lgDimensionData.getTransName()));
            lgDimensionData.disposeRawData();
        }

        for (var transName : transNames) {
            var transTDigest = transTDigests.get(transName);

            var bufSize = transTDigest.smallByteSize();
            var transTDigestBuffer = ByteBuffer.allocate(bufSize);
            transTDigest.asSmallBytes(transTDigestBuffer);

            transTDigestsBuffers.put(transName, transTDigestBuffer);
        }
    }

    public void rewindBuffers() {
        for (var lgDimensionData : lgDimensionsData) {
            lgDimensionData.rewindBuffer();
        }
    }

    public void rewindTransBuffers() {
        for (var tDigestsBuffer : transTDigestsBuffers.values()) {
            tDigestsBuffer.position(0);
        }
    }

    public Set<String> getDimensionsIds() {
        var dimensionsIds = new HashSet<String>();
        for (var lgDimensionData : lgDimensionsData) {
            dimensionsIds.add(lgDimensionData.getDimensionId());
        }
        return dimensionsIds;
    }

    public Set<String> getTransNames() {
        var transNames = new HashSet<String>();
        for (var lgDimensionData : lgDimensionsData) {
            transNames.add(lgDimensionData.getTransName());
        }
        return transNames;
    }

    public int getRegion() {
        return region;
    }

    public ArrayList<LgDimensionData> getLgDimensionsData() {
        return lgDimensionsData;
    }

    public Map<String, ByteBuffer> getTransTDigestsBuffers() {
        return transTDigestsBuffers;
    }
}