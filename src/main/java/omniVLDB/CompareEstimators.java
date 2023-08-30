package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.HYDRA_VLDB.HYDRA_VLDB.ImpHydraStruct;
import omniVLDB.omni.OmniSketch;
import omniVLDB.SketchZero.SketchZero;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CompareEstimators {
    // Class that compares the baselines
    Helper h;
    Dataset d;
    RamToPar rtp;
    //int[] ramVals = {(int) (80*1E6), (int) (80*2E6), (int) (80*3E6), (int) (80*4E6), (int) (80*5E6),
    //       (int) (80*10E6), (int) (80*15E6), (int) (80*20E6)};//{5, 10, 50, 100, 150, 200, 300, 500, 750, 1000};// TODO: make sure these are okay ram vals
    long[] ramVals = {(long) (10*8E6), (long) (50*8E6), (long) (100*8E6), (long) (200*8E6)};
    String[] conditions;
    public boolean[] attributesToRead;
    public CompareEstimators(Helper h) {
        this.h = h;

    }

    public void run() throws IOException, CsvValidationException {
        readDatasetSettings();
        Main.eps = 0.1;
        Main.delta = 0.1;

        for (int i = 0; i < conditions.length; i++) {
            if (Main.datasetName.equals("CAIDA")) {
                Main.filesToRead = Integer.parseInt(conditions[i]);
            }
            System.out.println("Running condition " + conditions[i]);


            readDataset(i);

            int numAttrsToUse = 0;
            for (boolean attr : d.attributesInWorkload) {
                if (attr)
                    numAttrsToUse++;
            }
            this.rtp = new RamToPar(numAttrsToUse, ramVals, Main.eps, Main.delta);
            Main.logger.info("Running dataset " + (i + 1) + " of " + conditions.length);
            System.out.println("Running dataset " + (i + 1) + " of " + conditions.length);
            System.out.println("Sketch0Min");
            Main.logger.info("Sketch0Min");
            runSketch0(false);

            System.out.println("Sketch0Cap");
            Main.logger.info("Sketch0Cap");
            runSketch0(true);
            for (long ram: ramVals) {
                Main.logger.info("Running sketch with ram " + ram);
                System.out.println("Running sketch with ram " + ram);
                Main.logger.info("OMNISKETCH");
                System.out.println("OMNISKETCH");
                runSketch(ram, rtp);
//                Main.logger.info("Hydra");
//                System.out.println("Hydra");
//                runHydra(ram, rtp);

            }
        }
    }

    private void runSketch0(boolean queryCap) throws IOException {
        Main.depth = (int) Math.ceil((Math.log(1 / Main.delta) /Math.log(Math.exp(1))));
        Main.width = (int) Math.ceil((Math.E / Main.eps));
        SketchZero sz = new SketchZero(d.attributesInWorkload, queryCap);
        runSynopsisRamBased(sz);
    }

    public void runSketch(long ram, RamToPar rtp) throws IOException {
        double[] params = rtp.getParamsSketch(ram);
        Main.depth = (int) params[0];
        Main.width = (int) params[1];
        Main.maxSize = (int) params[2];
        Main.b = (int) params[3];
        Main.s = new OmniSketch(d.attributesInWorkload, ram);
        runSynopsisRamBased(Main.s);
    }

    public void runHydra(long ram, RamToPar rtp) throws IOException {
        double[] params = rtp.getParamsHydra(ram);
        int depthRoot = (int) params[0];
        int widthRoot = (int) params[1];
        int depthCM = (int) params[2];
        int widthCM = (int) params[3];
        //Hydra hydra = new Hydra(ram, depthRoot, widthRoot, depthCM, widthCM);
        ArrayList<Integer> attrsIdx = new ArrayList<>();
        for (int i = 0; i < attributesToRead.length; i++) {
            if (attributesToRead[i])
                attrsIdx.add(i);
        }
        ImpHydraStruct hydra = new ImpHydraStruct(ram, depthRoot, widthRoot, depthCM, widthCM, attrsIdx);
        Main.s = hydra;
        runSynopsisRamBased(hydra);
    }

    public void runSynopsisRamBased(Synopsis syn) throws IOException {
        long time_passed = 0;
        long start = System.currentTimeMillis();
        for (Record r : d.getOrderedDataset()) {
            syn.add(r);
        }
        long end = System.currentTimeMillis();
        time_passed = end - start;
        Main.logger.info("Time passed for updates synopsis " + syn.setting + " is: " + time_passed + " ms, average: " + (double) time_passed / d.size + " ms");
        Main.logger.info("Memory usage synopsis " + syn.setting + ": " + syn.getMemoryUsage());
        Main.logger.info("Memory usage dataset: " + d.getMemoryUsage());

        System.out.println("Time passed for updates synopsis " + syn.setting + " is: " + time_passed + " ms, average: " + (double) time_passed / d.size + " ms");
        System.out.println("Memory usage synopsis " + syn.setting + ": " + syn.getMemoryUsage());
        System.out.println("Memory usage dataset: " + d.getMemoryUsage());
        System.out.println("\n");
        AnalysisBaselines ab = new AnalysisBaselines(syn, d, h, time_passed);
        ab.run();
        syn.reset();
        //ConditionChecks.run(d, s);
    }

    public void readDataset(int i) throws IOException, CsvValidationException {
        if (Main.datasetName.equals("SNMP")) {
            Main.fileStartCondition = conditions[i];
        } else {
            // Convert conditions[i] to int
            Main.numFiles = Integer.parseInt(conditions[i]);
        }
        boolean[] attrsToUse = new boolean[Main.numAttributes];
        if (Main.datasetName.equals("SNMP")) {
            ArrayList<Integer> order = new ArrayList<>(Arrays.asList(12, 14, 11, 1, 5, 13, 9, 8, 10, 21, 15));

            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = order.contains(j);
            }
        } else if (Main.datasetName.equals("CAIDA")) {
            ArrayList<Integer> order = new ArrayList<Integer>(Arrays.asList(7, 8, 5,9, 4));
            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = order.contains(j);
            }
            Main.filesToRead = Integer.parseInt(conditions[i]);
            Main.numFiles = Integer.parseInt(conditions[i]);}
        else {
            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = true;
            }
        }
        d = new Dataset(Main.numQueries, Integer.MAX_VALUE, Main.datasetName, Main.numAttributes, attrsToUse, h);
            //d.serialize(Main.filename);
//
//
//        ew = new ExpWorkload(d);
//       d.attributesInWorkload = ew.attributesUsed;
    }

    public void readDatasetSettings() {
        if (Main.datasetName.equals("SNMP")) {
            this.conditions = new String[]{"03110", "03110_OR_03111", "0311", "03", "0"};
            Main.numAttributes = 24;
        } else if (Main.datasetName.equals("CAIDA")) {
            this.conditions = new String[]{"1", "2", "3","4", "6", "8", "10"};//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
            Main.numAttributes = 11; //actually 7; can be 10;
        } else {
            throw new RuntimeException("Unknown dataset name");
        }
    }
}
