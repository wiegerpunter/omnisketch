package omniVLDB.omni;
import omniVLDB.DieHash;
import omniVLDB.Main;

import java.util.*;

public class DistinctSample extends Sample {
    int sampleLevel = 0;
    //int curSampleSize = 0;
    int intersectSize = 0;
    int t=1;
    TreeMap<Integer, HashSet<Integer>> sample = new TreeMap<Integer, HashSet<Integer>>();
    DieHash dieHash;

    public DistinctSample(DistinctSample ds){
        this.sampleLevel = ds.sampleLevel;
        this.curSampleSize = ds.curSampleSize;
        this.t = ds.t;
        this.dieHash = new DieHash(ds.dieHash);
        this.sample = new TreeMap<Integer, HashSet<Integer>>();
        for (Map.Entry<Integer, HashSet<Integer>> entry : ds.sample.entrySet()) {
            this.sample.put(entry.getKey(), new HashSet<Integer>(entry.getValue()));
        }
    }

    public DistinctSample(DieHash dieHash) {
        this.dieHash = dieHash;
    }

    public void add (int id) {
        int level = this.dieHash.hash(id);
        if (level >= sampleLevel) {
            this.sample.computeIfAbsent(level, k -> new HashSet<Integer>());
            this.sample.get(level).add(id); // Add id at specific level
            this.curSampleSize++;
        }
        if (this.curSampleSize > Main.maxSize) { // Remove records at lowest level if size of sample too high.
            remove();
        }
        assert this.curSampleSize <= Main.maxSize;
    }

    @Override
    public void add(long hx) {

    }

    public void add(int id, int hx) {
        int level = this.dieHash.hash(id);
        if (level >= sampleLevel) {
            this.sample.computeIfAbsent(level, k -> new HashSet<Integer>());
            this.sample.get(level).add(id); // Add id at specific level
            this.curSampleSize++;
        }
        if (this.curSampleSize > Main.maxSize) { // Remove records at lowest level if size of sample too high.
            remove();
        }
        assert this.curSampleSize <= Main.maxSize;
    }

    // remove records at lowest level if size of sample too high.
    public void remove() {
        if (sample.containsKey(sampleLevel)) {
            int levelSize = sample.get(sampleLevel).size();
            int pastSampleSize = curSampleSize;
            this.curSampleSize = pastSampleSize - levelSize;
            sample.get(sampleLevel).clear();
            sample.remove(sampleLevel);
        } else {
            throw new RuntimeException("Sample does not contain level " + sampleLevel);
        }
        sampleLevel++;
        while (!sample.containsKey(sampleLevel) && sampleLevel <= Main.maxLevel){
            sampleLevel++;
        }
        //if (sampleLevel > Main.maxLevel) {

            //throw new RuntimeException("Sample empty after removing");
        //}

        /*
        int testSampleSize = 0;
        int[] keySet = this.sample.descendingKeySet().stream().mapToInt(i->i).toArray();
        for (int l: keySet) {
            testSampleSize += this.sample.get(l).size();
        }
        if (testSampleSize != this.curSampleSize) {
            throw new RuntimeException("Sample size not equal to sum of levels.");
        }*/
    }



    // Intersect two DistinctSamples such that the resulting DistinctSample is the intersection of the two input DistinctSamples and minimum level is max of the two merged samples.
    public DistinctSample intersect(DistinctSample other) {
        this.sampleLevel = Math.max(this.sampleLevel, other.sampleLevel);
        int newSize = 0;
        int[] keySet = this.sample.descendingKeySet().stream().mapToInt(i->i).toArray();
        for (int level: keySet) {
            if (level >= this.sampleLevel && other.sample.containsKey(level)) {
                //HashSet<Integer> levelList = this.sample.get(level).stream().filter(other.sample.get(level)::contains).collect(Collectors.toCollection(ArrayList::new)); //.distinct()
                this.sample.get(level).retainAll(other.sample.get(level));
                        //put(level, levelList);//.retainAll(other.sample.get(level));
                newSize += this.sample.get(level).size();
                if (newSize > Main.maxSize) {
                    throw new RuntimeException("Intersection larger than one sample.");
                }
                //this.curSampleSize = this.curSampleSize + sample.get(level).size() - levelSize;
                //while (curSampleSize > Main.maxSize) { // Remove records at lowest level if size of sample too high.
                 //   remove();
                //}
            } else {
                this.sample.remove(level);
//                levelsToRemove.add(level);
            }
        }
//        for (int level: levelsToRemove) {
//            sample.remove(level);
//        }
        // Get sample size of this.sample
        /*int testSampleSize = 0;
        for (int level: this.sample.descendingKeySet()) {
            testSampleSize += this.sample.get(level).size();
        }
        assert testSampleSize == newSize;
        */
        this.curSampleSize = newSize;

        return this;
    }

    public DistinctSample union(DistinctSample other) {
        this.sampleLevel = Math.max(sampleLevel, other.sampleLevel);
        ArrayList<Integer> levelsToRemove = new ArrayList<Integer>();
        int[] keySet = sample.descendingKeySet().stream().mapToInt(i->i).toArray();
        for (int level: keySet) {
            if (level >= sampleLevel && level >= other.sampleLevel && other.sample.containsKey(level)) {
                int levelSize = sample.get(level).size();
                for (int i: other.sample.get(level)) {
                    if (sample.containsKey(level)) {
                        if (!sample.get(level).contains(i)) {
                            sample.get(level).add(i);
                            curSampleSize++;
                            if (curSampleSize > Main.maxSize) {
                                remove();
                            }
                        }
                    }
                }
            } else {
                levelsToRemove.add(level);
            }
        }
        for (int level: levelsToRemove) {
            if (sample.containsKey(level)) {
                sample.get(level).clear();
                sample.remove(level);
            }
        }
        return this;
    }

    public void setIntersectSize(int intersectSize) {
        this.intersectSize = intersectSize;
    }

    public void reset() {
        this.sampleLevel = 0;
        this.curSampleSize = 0;
        this.t = 1;
        this.sample.clear();// = new TreeMap<Integer, HashSet<Integer>>();
    }
}
