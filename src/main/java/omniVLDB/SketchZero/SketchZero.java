package omniVLDB.SketchZero;

import omniVLDB.Record;
import omniVLDB.*;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

public class SketchZero extends Synopsis {
    CountMin[] CMSketches = new CountMin[Main.numAttributes];
    ArrayList<DieHash> dieHashFunctions = new ArrayList<>();
    boolean[] hasPredicate;
    boolean queryCap = false;

    public SketchZero(boolean[] hasPredicates) {
        super();
        setting = "SketchZero";
        System.out.println("Epsilon: " + Main.eps);
        System.out.println("Delta: " + Main.delta);
        System.out.println("Depth: " + Main.depth);
        System.out.println("Width: " + Main.width);
        this.hasPredicate = ArrayUtils.clone(hasPredicates);
        //testDieHashes();
        for (int i = 0; i < Main.numAttributes; i++) {
            if (hasPredicates[i]) {
                CMSketches[i] = new CountMin(i);
            } else {
                CMSketches[i] = null;
            }
            //CMSketches.add(new CountMin(dieHashFunctions, i));
        }
    }

    public SketchZero(boolean[] hasPredicates, boolean queryCap) {
        this.queryCap = queryCap;
        if (queryCap) {
            this.setting = "Sketch0Cap";
        } else {
            this.setting = "Sketch0Min";
        }
        System.out.println("Depth: " + Main.depth);
        System.out.println("Width: " + Main.width);
        this.parameters = new int[]{Main.depth, Main.width};
        this.hasPredicate = ArrayUtils.clone(hasPredicates);
        //testDieHashes();
        for (int i = 0; i < Main.numAttributes; i++) {
            if (hasPredicates[i]) {
                CMSketches[i] = new CountMin(i);
            } else {
                CMSketches[i] = null;
            }
            //CMSketches.add(new CountMin(dieHashFunctions, i));
        }
    }

    public void add(Record record) {
        //long start = System.currentTimeMillis();
        for (int i = 0; i < Main.numAttributes; i++) {
            if (hasPredicate[i]) {
                CMSketches[i].add(record.getId(), record.getRecord()[i]);
            }
        }
        //long end = System.currentTimeMillis();
        //System.out.println("Add time: " + (end - start));
    }

    int minCMRow = 0;
    public int query(Query q) {
        if (queryCap) {
            return queryCap(q);
        } else {
            return queryMin(q);
        }
    }
    public int rangeQuery(Query q) {
        return 0;
    }

    int queryCap(Query q) {
        double S_cap = 0;
        TreeSet<Integer>[] treeSets = new TreeSet[q.predAttrs.size() * Main.depth];
        for (int i = 0; i < q.predAttrs.size(); i++) {
            TreeSet<Integer>[] temp = CMSketches[q.predAttrs.get(i)].query(q.getRecord()[q.predAttrs.get(i)]);
            if (Main.depth >= 0) System.arraycopy(temp, 0, treeSets, i * Main.depth, Main.depth);
        }
        S_cap = getAltEstKMV(treeSets); //Instead of getAltEstKMV
        q.intersectSize = S_cap;
        return (int) S_cap;

    }

    private double getAltEstKMV(TreeSet<Integer>[] samples) {
        int numJoins = samples.length;
        int c = 0;
        Iterator<Integer> iter = samples[0].iterator();
        while (iter != null && iter.hasNext()) {
            boolean found = true;
            Integer i = iter.next();
            for (int j = 1; j < numJoins; j++) {
                Integer otherElement = samples[j].ceiling(i);
                if (otherElement == null) {
                    found = false;
                    iter = null;
                    break;
                } // not contained
                else if (otherElement.equals(i)) continue; // is contained
                else {
                    iter = samples[0].tailSet(otherElement).iterator(); // fast forward iter0
                    found = false;
                    break; // but now you need to start from iter.hasNext() again
                }
            }
            if (found) c++;

        }
        return c;
    }

    public int queryMin(Query q) {
        //TreeSet<Integer>[][] treeSets = new TreeSet[q.predAttrs.size()][Main.depth];
        TreeSet<Integer>[][] intersectSampleRows = new TreeSet[Main.depth][q.predAttrs.size()];
        for (int i = 0; i < q.predAttrs.size(); i++) {
            TreeSet<Integer>[] temp = CMSketches[q.predAttrs.get(i)].query(q.getRecord()[q.predAttrs.get(i)]);
            if (Main.depth >= 0) {
                for (int j = 0; j < Main.depth; j++) {
                    intersectSampleRows[j][i] = temp[j];
                }
            }
        }
        int estimate = Integer.MAX_VALUE;
        for (int j = 0; j < Main.depth; j++) {
            double sizeAtThisRow = getAltEstKMV(intersectSampleRows[j]);
            if (sizeAtThisRow < estimate) {
                minCMRow = j;
                estimate = (int) sizeAtThisRow;
                q.intersectSize = sizeAtThisRow;
            }
        }
        return estimate;
    }

    public long getMemoryUsage() {
        long memoryUsage = 0;
        for (int i = 0; i < Main.numAttributes; i++) {
            if (hasPredicate[i]) {
                memoryUsage += CMSketches[i].getMemoryUsage();
            }
        }
        memUsageSynopsis = memoryUsage;
        return memoryUsage;
    }

    public void reset() {
        for (int i = 0; i < Main.numAttributes; i++) {
            if (hasPredicate[i]) {
                CMSketches[i].reset();
            }
        }
    }


}
