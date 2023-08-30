package omniVLDB;

import java.util.HashMap;

public class RamToPar {

    long[] ramVals;

    double[] epsVals = {1e-11, 1e-10, 1e-9, 0.00000001, 0.00000002, 0.00000003, 4e-8, 5e-8, 6e-8, 7e-8, 8e-8, 9e-8, 0.0000001, 0.000001, 0.00001,  0.0001,0.001,0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15,
    0.16, 0.17, 0.18, .19, .20, .21, .22, .23, .24, .25, .26, .27, .28, .29, .30, .31, .32, .33, .34, .35, .36, .37, .38, .39,
            .4, .41, .42, .43, .44, .45, .46, .47, .48, .49, .5, .51, .52, .53, .54, .55, .56, .57, .58, .59,
            .6, .61, .62, .63, .64, .65, .66, .67, .68, .69, .7, .71, .72, .73, .74, .75, .76, .77, .78, .79, .8,
            .81, .82, .83, .84, .85, .86, .87, .88, .89, .9, .91, .92, .93, .94, .95, .96, .97, .98, .99};
    HashMap<Long, double[]> ramToSketchParams = new HashMap<>();
    HashMap<Long, double[]> ramToHydra = new HashMap<>();
    int numAttrs;
    public RamToPar(int numAttrsToUse, long[] ramValsToUse) {
        this.numAttrs = numAttrsToUse;
        this.ramVals = ramValsToUse;
        run();
    }

    public RamToPar(int numAttrsToUse,long[] ramValsToUse, double eps, double delta) {
        this.numAttrs = numAttrsToUse;
        this.ramVals = ramValsToUse;
        Main.eps = eps;
        Main.delta = delta;
        for (long r: ramVals) {
            double[] pars = getInfoViaB(r, eps);
            System.out.println("Info, Ram: " +  r + " usedRAM: "+ pars[0] + " B: " + pars[3] + " b: " + pars[4] + " width: " + pars[2] + " depth: " + pars[1]);
            ramToSketchParams.put(r, pars);
        }

        // Add params for hydra to ramToHydra based on main eps main delta
        for (long r: ramVals) {
            double[] pars = gridSearchHydra(r);
            ramToHydra.put(r, pars);
        }
    }

    public void run() {
        for (long ram : ramVals) {
            double[] pars = gridSearchSketch(ram);
            ramToSketchParams.put(ram, pars);

            double[] parsHydra = gridSearchHydra(ram);
            ramToHydra.put(ram, parsHydra);
        }
        System.out.println("Done with grid search");

    }

    private double[] gridSearchSketch(long ram) {
        int maxSize = 0;
        double[] currentInfo = new double[7];
        double[] epsValsSketch = {0.05, 0.1, 0.2, 0.3, 0.4, 0.5};

        if (Main.sanityBound) {
            for (double eps : epsValsSketch) {
                double[] info = getSizeSketch(ram, eps);
                if (info[0] > maxSize) {
                    maxSize = (int) info[0];
                    currentInfo = info;
                }
            }
        } else {
            double eps = 0.1;
            Main.eps = eps;
            currentInfo = getInfoViaB(ram, eps);
            System.out.println("Info, Ram: " +  ram + " usedRAM: "+ currentInfo[0] + " B: " + currentInfo[3] + " b: " + currentInfo[4] + " width: " + currentInfo[2] + " depth: " + currentInfo[1]);
        }
        if (currentInfo[0] == 0) {
            System.out.println("No sketch found for ram " + ram + " and eps" + currentInfo[5] + " and depth " + currentInfo[1] + " and width " + currentInfo[2]);
        }
        return currentInfo;
    }

    private double[] getInfoViaB(long ram, double eps) {

        double deltaCM =  Main.delta/2;
        Main.deltaDS = Main.delta/2;
        int depth = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1)));
        System.out.println("Depth: " + depth);
        double epsCMpowD = eps /(1 + eps);
        double epsCM = Math.pow(epsCMpowD, 1.0/depth);
        System.out.println("epsCM: " + epsCM);
        //double epsCM = eps;
        double epsDS = Math.pow(eps, depth);
        double[] info = new double[7];
        //int depth = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1))); // Depth of sketch (number of hash functions)
        int width =  1 + (int) Math.ceil(Math.exp(1)/epsCM);
        System.out.println("Width: " + width);// Width of sketch (number of buckets)
        DetermineB determineB = new DetermineB(ram);
        int B = determineB.determineB(depth, width, numAttrs, Main.delta);
        System.out.println("B: " + B);
        int b = (int) Math.ceil(Math.log(4*Math.pow(B, (double) 5/2)/Main.delta));
        System.out.println("b: " + b);
        //double memUsage = (double) (depth * width*(B *((double) Main.b/32 * 4) + 4) * numAttrs);//*Main.numAttributes);
        double memUsage;
        if (Main.rangeQueries) {
            memUsage = Main.dyadicRangeBits / Math.log(2) * depth * width * numAttrs * (B * (b + 3 * 32 + 1) + 32);
        } else {
            memUsage = (double) depth * width * numAttrs * (B * (b + 3 * 32 + 1) + 32);
        }
        System.out.println("MemUsage: " + memUsage);
        // Memory = depth * width * ( sample size * small b size * bits needed for pointers + bits needed for counters ) * number of attributes
        if (memUsage < ram) {
            info = new double[]{memUsage, depth, width, B, b, eps, epsCM, epsDS};
       }
       return info;
    }

    private double[] gridSearchHydra(long ram) {
        int maxSize = 0;
        double[] currentInfo = new double[8];
        for (double eps : epsVals) {
            double[] info = getSizeHydra(ram, eps);
            if (info[0] > maxSize) {
                maxSize = (int) info[0];
                currentInfo = info;
            }
        }
        return currentInfo;
    }

    private double[] getSizeSketch(long ram, double eps) {
        double deltaCM = 0.05;
        double deltaDS = 0.05;
        Main.deltaDS = deltaDS;
        int depth = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1)));
        //double epsCM = Math.pow((Math.pow(eps, depth)/(1 + Math.pow(eps, depth))), (double) (1/depth));
        double epsCMpowD = eps / (1 + eps);
        double epsCM = Math.pow(epsCMpowD, 1.0/depth);
        double epsDS = Math.pow(eps, depth);
        double[] info = new double[7];
        //int depth = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1))); // Depth of sketch (number of hash functions)
        int width =  1 + (int) Math.ceil(Math.exp(1)/epsCM); // Width of sketch (number of buckets)
        double numerator;
        double denominator;
        double ratio = 1/Main.qTarget;
        int B;
        int b;
        long memUsage;
        if (Main.useDS) {
            numerator = Math.log(1/deltaDS)/Math.log(Math.exp(1));
            denominator = Math.pow(epsDS, 2);
            B = (int) Math.ceil(12 * (numerator / denominator) * ratio);
            memUsage = (long) depth*width*B*numAttrs* 4;
        } else {
            numerator = Math.log(2* numAttrs *depth/deltaDS)/Math.log(Math.exp(1));
            denominator = Math.pow(epsDS, 2);
            B = (int) Math.ceil(2 * (numerator / denominator) * ratio);
            b = (int) Math.ceil(Math.log(4*Math.pow(B, (double) 5/2)/deltaDS));
            memUsage = (long) (depth * width*(((long) B *b + 3 * 32 + 1) + 32) * numAttrs);
            // Memory = depth * width * ( sample size * small b size * bits needed for pointers + bits needed for counters ) * number of attributes
        }
        if (memUsage < ram) {
            info = new double[]{memUsage, depth, width, B, eps, epsCM, eps};
        }
        return info;
    }

    private double[] getSizeHydra(long ram, double eps) {
        double delta = Main.delta/2;
        double deltaCM = Main.delta/2;
        //double epsCM = eps/2;
        //double epsRoot = eps/2;
        double[] info = new double[8];
        int depthCM = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1))); // Depth of sketch (number of hash functions)
        //int widthCM = (int) Math.ceil(Math.exp(1)/Math.pow(eps, 2)); // Width of sketch (number of buckets)
        int depthRoot = (int) Math.ceil(Math.log(1/delta)/Math.log(Math.exp(1))); // Depth of sketch (number of hash functions)
        //int widthRoot = (int) Math.ceil(Math.exp(1)/epsRoot); // Width of sketch (number of buckets)
        int memLeft = (int) Math.ceil((double) ram/(32*depthCM*depthRoot));
        int widthCM = (int) Math.pow((double) memLeft/2000, 0.66);
        int widthRoot = (int) Math.pow(Math.sqrt(memLeft) * 2000, 0.66);
        long memUsage = (long) depthCM*widthCM*depthRoot*widthRoot*32;
        if (memUsage < ram) {
            info = new double[]{memUsage, depthCM, widthCM, depthRoot, widthRoot, eps};
        } else {
            System.out.println("memUsage: " + memUsage + " ram: " + ram);
        }
        return info;
    }

    public double[] getParamsSketch(long ram) {
        //HashMap<Integer, double[]> ramToParams;
        if (ramToSketchParams.containsKey(ram)) {
            double[] info = ramToSketchParams.get(ram);
            double d = info[1];
            double w = info[2];
            double B = info[3];
            double b = info[4];
            System.out.println("d: " + d + " w: " + w + " B: " + B + " b: " + b);
            return new double[]{d, w, B, b};
        } else {
            throw new IllegalArgumentException("RAM not found in ramToSketchParams");
//            long closestKey = getClosestKey(ram, ramToSketchParams);
//            double[] info = ramToSketchParams.get(closestKey);
//            double d = info[1];
//            double w = info[2];
//            double b = info[3];
//            return new double[]{d, w, b};
        }
    }

    private long getClosestKey(long ram, HashMap<Long, double[]> ramToParams) {
        long closestKey = 0;
        long minDiff = Long.MAX_VALUE;
        for (long key : ramToParams.keySet()) {
            long diff = Math.abs(key - ram);
            if (diff < minDiff) {
                minDiff = diff;
                closestKey = key;
            }
        }
        return closestKey;

    }

    public double[] getParamsHydra(long ram) {
        if (ramToHydra.containsKey(ram)) {
            double[] info = ramToHydra.get(ram);
            double dRoot = info[1];
            double wRoot = info[2];
            double dCM = info[3];
            double wCM = info[4];
            System.out.println("d Root: " + dRoot + " w Root: " + wRoot + " d CM: " + dCM + " w CM: " + wCM);
            return new double[]{dRoot, wRoot, dCM, wCM};
        } else {
            long closestKey = getClosestKey(ram, ramToHydra);
            double[] info = ramToHydra.get(closestKey);
            double dRoot = info[1];
            double wRoot = info[2];
            double dCM = info[3];
            double wCM = info[4];
            return new double[]{dRoot, wRoot, dCM, wCM};
        }
    }
}
