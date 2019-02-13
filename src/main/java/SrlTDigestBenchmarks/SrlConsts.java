package SrlTDigestBenchmarks;

public class SrlConsts {

    public final static int TdCompression = 100;
    public final static int TdAggCompression = 500;
    public final static int MaxLgs = 2000;
    public final static int MaxEmulations = 5;
    public final static int MaxTransactions = 2000;
    public final static int MaxTransactionsPerLgPerInterval = MaxTransactions / 2;
    public final static int MaxRegions = 10;
    public final static int MaxDimensions = MaxRegions * MaxTransactions * MaxEmulations;
    public final static int LgValuesPerDimension = 80;
    public final static int MaxDimensionsPerLg = MaxTransactionsPerLgPerInterval * MaxEmulations;
    public final static int DataSizeInLg = MaxDimensionsPerLg * LgValuesPerDimension;
    public final static int ResultsAggregatorsNum = 8;
    public final static int TDigestsPerResultsAggregator = MaxLgs / ResultsAggregatorsNum;
}
