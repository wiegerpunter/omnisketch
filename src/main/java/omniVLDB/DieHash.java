package omniVLDB;

import java.util.Random;

public class DieHash {
    int m;
    int a;
    int b;
    //int c;

    public DieHash(int m, int seed) {
        this.m = m;
        Random r = new Random(seed);
        this.a = r.nextInt();
        this.b = r.nextInt();
//        testDieHashDistribution();
        //System.out.println("DieHash: m = " + m + ", a = " + a + ", b = " + b);
    }

    public DieHash(DieHash dieHash) {
        this.m = dieHash.m;
        this.a = dieHash.a;
        this.b = dieHash.b;
        //this.c = dieHash.c;
    }

    public int hash(long id) {
        int x = Math.floorMod((a*id + b), (int) Math.pow(2, m - 1));
        //int x = (int) (a*id + b) % (int) Math.pow(2, m - 1);
        if (x < 0) {
            throw new RuntimeException("x is negative");
        }
        int level;
        if (x == 0) {
            level = m - 1;
        } else {

            level = (m - (int) Math.ceil(Math.log(x)/Math.log(2))) - 1;
        }
        if (level > m) {
            throw new RuntimeException("Level too high");
        }
        if (level < 0) {
            throw new RuntimeException("Level negative ("+ level + ") for id " + id + " and x " + x + " and m " + m + " and a " + a + " and b " + b);
        }
        return level;
    }

    // Function that tests the distribution of hash function
    public void testDieHashDistribution() {
        int[] counts = new int[m];
        for (int i = 0; i < 1000000000; i++) {
            if ((hash(i) >= m) || (hash(i) < 0)) throw new RuntimeException("Level too high in testDieHashDistribution for i: " + i + " and level: " + hash(i));
            counts[hash(i)]++;
        }
        for (int i = 0; i < m; i++) {
            System.out.println("Level " + i + ": " + counts[i]);
        }
    }
}

