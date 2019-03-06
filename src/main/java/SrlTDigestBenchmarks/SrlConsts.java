package SrlTDigestBenchmarks;

public class SrlConsts {

    public final static int TdCompression = 100;
    public final static int TdAggCompression = 200;
    public final static int MaxLgs = 20;
    public final static int MaxEmulations = 3;
    public final static int MaxTransactions = 1500;
    public final static int MaxTransactionsPerLgPerInterval = MaxTransactions / 2;
    public final static int MaxRegions = 4;
    public final static int MaxDimensions = MaxRegions * MaxTransactions * MaxEmulations;
    public final static int LgValuesPerDimension = 120;
    public final static int MaxDimensionsPerLg = MaxTransactionsPerLgPerInterval * MaxEmulations;
    public final static int DataSizeInLg = MaxDimensionsPerLg * LgValuesPerDimension;
    //public final static int ResultsAggregatorsNum = 16;
    //public final static int TDigestsPerResultsAggregator = MaxLgs / ResultsAggregatorsNum;
    //public final static int DimenstionsPerAgg = SrlConsts.MaxDimensions / SrlConsts.ResultsAggregatorsNum;
}
