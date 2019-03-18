package Common;

public class SrlConsts {

    public final static int NumIterations = 5;
    //public final static int AggDurationMilliSeconds = 60000;
    public final static int AggDurationMilliSeconds = 1500;
    public final static int TdCompression = 100;
    public final static int TdAggCompression = 200;
    public final static int MaxLgs = 10;
    public final static int MaxEmulations = 1;
    public final static int MaxTransactions = 1000;
    public final static int MaxTransactionsPerLgPerInterval = MaxTransactions / 2;
    public final static int MaxRegions = 1;
    public final static int MaxDimensions = MaxRegions * MaxTransactions * MaxEmulations;
    public final static int LgValuesPerDimension = 190;
    public final static int MaxDimensionsPerLg = MaxTransactionsPerLgPerInterval * MaxEmulations;
    public final static int DataSizeInLg = MaxDimensionsPerLg * LgValuesPerDimension;
    //public final static int ResultsAggregatorsNum = 16;
    //public final static int TDigestsPerResultsAggregator = MaxLgs / ResultsAggregatorsNum;
    //public final static int DimenstionsPerAgg = SrlConsts.MaxDimensions / SrlConsts.ResultsAggregatorsNum;
}
