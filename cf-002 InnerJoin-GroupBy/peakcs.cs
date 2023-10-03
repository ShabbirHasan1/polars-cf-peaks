using peakcs;

class Program
{
    static void Main(string[] args)
    {
        var startTime = DateTime.Now;
       
        Peakcs.DataFrame df = new Peakcs.DataFrame
        {
            LogFileName = "Outbox/Log-" + startTime.ToString("yyMMdd-HHmmss") + ".csv",
            PartitionSizeMB = 5,
            Thread = 100
        };      

        Peakcs.CreateLog(df);

        var query = Peakcs.Query(
            "filter", "Style(=F)",
            "build_key_value", "Product, Style => Table(key_value)");

        var masterDf = Peakcs.RunBatch(df, "Inbox/Master.csv", query);

        query = Peakcs.Query(
           "filter", "Shop(S90..S99)",
           "join_key_value", "Product, Style => Inner(key_value)",
           "add_column", "Quantity, Unit_Price => Multiply(Amount)",
           "filter", "Amount:Float(>100000)",
           "group_by", "Shop, Product => Count() Sum(Quantity) Sum(Amount)");

        var sourceFile = Peakcs.GetCliFilePath("10-MillionRows.csv");
        var resultFile = new string[]
        {
            "Outbox/Peakcs-Detail-Result-" + Path.GetFileName(sourceFile),
            "Outbox/Peakcs-Summary-Result-" + Path.GetFileName(sourceFile)
        };

        Peakcs.RunStream(df, masterDf, sourceFile, query, resultFile);

        Peakcs.ViewSample(resultFile[0]);
        Peakcs.ViewSample(resultFile[1]);

        var endTime = DateTime.Now;
        var duration = endTime - startTime;

        Console.WriteLine($"Peakcs Duration (In Second): {duration.TotalSeconds}");               
    }
}