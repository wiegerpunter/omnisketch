package omniVLDB.omni;

import omniVLDB.DieHash;
import omniVLDB.Main;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

public class CountMinDyad {

    //ArrayList<ArrayList<Sample>> CM = new ArrayList<>();

    Kmin[][] CM = new Kmin[Main.depth][Main.width];
    ArrayList<DieHash> dieHashFunctions;
    //ArrayList<UniformHash> hashFunctions;

    int attr;
    long intervalSize;
    final Random rn = new Random();
    int n = 0;

    public CountMinDyad(int attr, long intervalSize) {
        this.attr = attr;
        this.intervalSize = intervalSize;
        initSketch();
    }


    public void initSketch() {
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                CM[j][i] = new Kmin(Main.deltaDS);
            }
        }
    }

    int[] hash(long attrValue, int depth, int width) {
        int[] hash = new int[depth];
        //int attrValueInt = (int) attrValue;
        rn.setSeed(attrValue + Main.repetition);
        for (int i = 0; i < depth; i++) {
            hash[i] = rn.nextInt(width);
        }
        return hash;
    }
    public long getRangeSignature(long start, long stop) {
        long sum = start;
        sum<<=Main.dyadicRangeBits;
        sum |= stop;
        return sum;
    }

    public void add(long lower, long higher, long hx) {
        // Test if all element in A and B are consistent
        long sig = getRangeSignature(lower, higher);
        //if (intervalSize == 34 && attr == 1) {
        //    System.out.println("sig: " + sig);
        //}

//        if (intervalSize == 1 && attr == 8) {
//            if (lower != 8594268983L) {
//                System.out.println("lower: " + lower);
//            }
//            System.out.println("sig: " + sig);
//        }
        int[] hashes = hash(sig, Main.depth, Main.width);
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            CM[j][w].add(hx); // Hash in Sample based on id
        }
        n += 1;
    }

    public TreeSet<Long>[] rangeQuery(long lower, long higher, int[] seenN) {
        TreeSet<Long>[] set = new TreeSet[Main.depth];
        long sig = getRangeSignature(lower, higher);
        int[] hashes = hash(sig, Main.depth, Main.width);
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            set[j] = new TreeSet<>(CM[j][w].sketch);
            seenN[j] += CM[j][w].n;
            // Hash in Sample based on id
        }
        return set;
    }
    public long getMemoryUsage() {
        long memoryUsage = 0;
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                memoryUsage += CM[j][i].getMemoryUsage();
            }
        }
        return memoryUsage;
    }

    public void reset() {
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                CM[j][i].reset();
            }
        }
    }

    public void testUniformity() {
        System.out.println("Testing Uniformity");
        int[] counts = new int[Main.width];
        for (int i = 0; i < Main.width; i++) {
            counts[i] = 0;
        }
        for (int i = 0; i < 1000000; i++) {
            int[] hashes = hash(i, Main.depth, Main.width);
            for (int j = 0; j < Main.depth; j++) {
                int w = hashes[j];
                counts[w]++;
            }
        }
        for (int i = 0; i < Main.width; i++) {
            System.out.println(counts[i]);
        }
    }


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