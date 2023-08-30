package omniVLDB;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfGenerator {
    public static long[][] zipfData(int numberOfRecords,int numberOfAttributes, int maxValue, double alpha) {
        long[][] data = new long[numberOfRecords][numberOfAttributes];
        ZipfDistribution zipf = new ZipfDistribution(maxValue, alpha);
        for (int i=0;i<numberOfRecords;i++) for (int j=0;j<numberOfAttributes;j++) data[i][j] = zipf.sample();
        return data;
    }
}
