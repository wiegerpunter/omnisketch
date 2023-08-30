package omniVLDB.SketchZero;
import omniVLDB.Main;
import java.util.Random;
import java.util.TreeSet;

public class CountMin {

    //ArrayList<ArrayList<Sample>> CM = new ArrayList<>();

    TreeSet<Integer>[][] CM = new TreeSet[Main.depth][Main.width];
    int attr;
    final Random rn = new Random();

    public CountMin(int attr) {
        this.attr = attr;
        initSketch();
    }


    public void initSketch() {
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                CM[j][i] = new TreeSet<>();
            }
        }
    }

    int[] hash(long attrValue, int depth, int width) {
        int[] hash = new int[depth];
        rn.setSeed(attrValue + Main.repetition);
        for (int i = 0; i < depth; i++) hash[i] = rn.nextInt(width);
        return hash;
    }

    public void add(int id, long attrValue) {
        // Test if all element in A and B are consistent
        int[] hashes = hash(attrValue, Main.depth, Main.width);
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            CM[j][w].add(id); // Hash in Sample based on id
        }
    }

    public TreeSet<Integer>[] query(long attrValue) {
        int[] hashes = hash(attrValue, Main.depth, Main.width);

        TreeSet<Integer>[] result;
        result = new TreeSet[Main.depth];
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            result[j] = CM[j][w];
        }
        return result;
    }

    public long getMemoryUsage() {
        long memoryUsage = 0;
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                memoryUsage += CM[j][i].size() * (4 * 32L + 1);
            }
        }
        return memoryUsage;
    }

    public void reset() {
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                CM[j][i].clear();
            }
        }
    }

    /*public Sample[] queryRange(Long min, Long max) {
        Sample[] result = new Sample[Main.depth];
        for (long i = min; i <= max; i++) {
            if (i == min) {
                result = query(i);
            }
            Sample[] query = query(i);
            for (int j = 0; j < Main.depth; j++) {
                result[j].union(query[j]);
            }
        }
        return result;
    }*/


/*
    public int[] testAdd(int id, int attr) {
        return hash(attr, Main.depth, Main.width);
    }

    public void testUniformity() {
        System.out.println("Testing Uniformity");
        int[] counts = new int[Main.width];
        for (int i = 0; i < Main.width; i++) {
            counts[i] = 0;
        }
        for (int i = 0; i < 100000; i++) {
            int[] hashes = hash(i, Main.depth, Main.width);
            for (int j = 0; j < Main.depth; j++) {
                int w = hashes[j];
                counts[w]++;
            }
        }
        for (int i = 0; i < Main.width; i++) {
            System.out.println(counts[i]);
        }
    }*/

    /*
        Input: 1. Attribute
        Output: 2. Arraylist of size depth with all relevant Distinct samples for single attribute
     */
}