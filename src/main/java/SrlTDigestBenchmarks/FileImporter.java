package SrlTDigestBenchmarks;

import com.tdunning.math.stats.MergingDigest;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FileImporter {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: FileImporter fileName percentile compression");
            System.exit(-1);
        }

        var fileName = args[0];
        var tdigest = new MergingDigest(Integer.valueOf(args[2]));

        var heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        var memUsed = heapMemoryUsage.getUsed();
        System.out.println("Memory used = " + memUsed);

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            //stream.forEach(metric -> System.out.println(Double.valueOf(metric)));
            stream.forEach(metric -> tdigest.add(Double.valueOf(metric)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("90th = " + tdigest.quantile(Double.valueOf(args[1])));
        heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        memUsed = heapMemoryUsage.getUsed();
        System.out.println("Memory used = " + memUsed);
    }
}
