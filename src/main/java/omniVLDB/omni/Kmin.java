package omniVLDB.omni;

import omniVLDB.Main;

import java.util.*;

public class Kmin extends Sample {
    //PriorityQueue<Integer> sketch;
    public TreeSet<Long> sketch;
    //int sketchSize = 0;
    int K; // number of lowest values to store
    int n = 0;
    //int b;
    int simax = Integer.MIN_VALUE;

    int seed;
    Random rn = new Random();
    long maxHash = Math.min((int) Math.pow(2, Main.b), Integer.MAX_VALUE);
    long hash(int x) {
        long k;
        rn.setSeed(x + Main.repetition);
        // Hash function hashing x to [0, 1]^b with base = log(2m^2/delta)

        k = rn.nextLong(maxHash);

        return k;
    }

    /* public static int hash(int input, int b) {
        int hash = input ^ random.nextInt(); // XOR the input with a random number
        hash = hash % (1 << b); // Modulo operation to limit the hash to b bits
        return hash;
    }*/
    public Kmin() {
        this.K = Main.maxSize;
        //this.sketch = new PriorityQueue<>(Main.maxSize, Collections.reverseOrder());
        this.sketch = new TreeSet<>(Collections.reverseOrder());

        this.delta = Main.delta /2;
        //Main.b = (int) Math.ceil(Math.log(4*Math.pow(K,(double) 5/2)/this.delta));
    }
    public Kmin(double delta) {
        this.K = Main.maxSize;
        this.sketch = new TreeSet<>(Collections.reverseOrder());

        //this.sketch = new PriorityQueue<>(Main.maxSize, Collections.reverseOrder());
        this.delta = delta;
        //Main.b = (int) Math.ceil(Math.log(4*Math.pow(K,(double) 5/2)/this.delta));
    }

    public Kmin(Kmin other) {
        this.K = other.K;
        //this.sketch = new PriorityQueue<>(other.sketch);
        this.sketch = new TreeSet<>(other.sketch);
        this.curSampleSize = other.curSampleSize;
        this.delta = other.delta;
    }
    @Override
    public void add(int id) {
//        n++;
//        int hx = hash(id);
//        if (curSampleSize < K) {
//            sketch.add(hx);
//            curSampleSize++;
//        } else {
//            if (hx < curTreeRoot) { // get tree root
//                sketch.pollFirst();
//                sketch.add(hx);
//                curTreeRoot = sketch.first();
//            }
//        }
    }

    long curTreeRoot = Long.MAX_VALUE;
    public void add(long hx) {
        n++;
        if (curSampleSize < K) {
            sketch.add(hx);
            curSampleSize++;
        } else {
            if (hx < curTreeRoot) { // get tree root
                sketch.pollFirst();
                sketch.add(hx);
                curTreeRoot = sketch.first();
            }
        }
    }

    @Override
    public void reset() {
        //this.sketch = new PriorityQueue<>(Main.maxSize, Collections.reverseOrder());
        this.sketch = new TreeSet<>(Collections.reverseOrder());
        this.curSampleSize = 0;
    }

    @Override
    public long getMemoryUsage() {
        return (sketch.size() * (Main.b + 32*3 + 1) + 32);
    }
}
