package omniVLDB.omni;

import omniVLDB.DieHash;
import omniVLDB.Main;

import java.util.ArrayList;
import java.util.Random;

public class CountMin {

    //ArrayList<ArrayList<Sample>> CM = new ArrayList<>();

    Sample[][] CM = new Sample[Main.depth][Main.width];
    ArrayList<DieHash> dieHashFunctions;
    //ArrayList<UniformHash> hashFunctions;

    int attr;
    final Random rn = new Random();

    public CountMin(ArrayList<DieHash> dieHashFunctions, int attr) {
        this.dieHashFunctions = dieHashFunctions;
        this.attr = attr;
        //this.hashFunctions = new ArrayList<UniformHash>(depth);
        initSketch();
    }


    public void initSketch() {
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                if (Main.useDS) {
                    CM[j][i] = new DistinctSample(dieHashFunctions.get(j));
                } else {
                    CM[j][i] = new Kmin(Main.deltaDS);
                }
            }
        }
    }

    int[] hash(long attrValue, int depth, int width) {
        int[] hash = new int[depth];
        rn.setSeed(attrValue + Main.repetition);
        for (int i = 0; i < depth; i++) hash[i] = rn.nextInt(width);
        return hash;
    }

//    public void add(int id, long attrValue) {
//        // Test if all element in A and B are consistent
//        int[] hashes = hash(attrValue, Main.depth, Main.width);
//        for (int j = 0; j < Main.depth; j++) {
//            int w = hashes[j];
//            CM[j][w].add(id); // Hash in Sample based on id
//        }
//    }
    public void add(int id, long attrValue, long hx) {
        // Test if all element in A and B are consistent
        int[] hashes = hash(attrValue, Main.depth, Main.width);
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            CM[j][w].add(hx); // Hash in Sample based on id
        }
    }

    public Sample[] query(long attrValue) {
        int[] hashes = hash(attrValue, Main.depth, Main.width);

        Sample[] result;
        if (Main.useDS) {
            result = new DistinctSample[Main.depth];
        } else {
            result = new Kmin[Main.depth];
            //throw new RuntimeException("KMV not implemented");
        }
        for (int j = 0; j < Main.depth; j++) {
            int w = hashes[j];
            result[j] = CM[j][w];
            //result.add(new DistinctSample(CM.get(j).get(w)));
        }
        return result;
    }

    public long getMemoryUsage() {
        long memoryUsage = 0;
        for (int j = 0; j < Main.depth; j++) {
            for (int i = 0; i < Main.width; i++) {
                //System.out.println("CM[" + j + "][" + i + "].curSampleSize = " + CM.get(j).get(i).curSampleSize);
                if (Main.useDS) {
                    memoryUsage += (long) (CM[j][i].curSampleSize * 32L);
                } else {
                    memoryUsage += CM[j][i].getMemoryUsage();
                }
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


    /*
        Input: 1. Attribute
        Output: 2. Arraylist of size depth with all relevant Distinct samples for single attribute
     */
}