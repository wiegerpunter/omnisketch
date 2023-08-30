package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.omni.OmniSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Scalability {
    // Class that compares the baselines
    Helper h;
    Dataset d;
    RamToPar rtp;
    long[] ramVals = {(long) (50*8E6), (long) (100*8E6), (long) (200*8E6)};
    String[] conditions;
    public Scalability(Helper h) {
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
            readDataset(i);

            int numAttrsToUse = 0;
            for (boolean attr : d.attributesInWorkload) {
                if (attr)
                    numAttrsToUse++;
            }
            if (Main.rangeQueries) {
                if (Main.datasetName.equals("SNMP")) {
                    int[] order = {12, 14, 11, 1, 5, 13, 9, 8, 10, 21, 15};
                    numAttrsToUse = order.length;
                }
            }
            this.rtp = new RamToPar(numAttrsToUse, ramVals, Main.eps, Main.delta);
            Main.logger.info("Running dataset " + (i + 1) + " of " + conditions.length);
            System.out.println("Running dataset " + (i + 1) + " of " + conditions.length);
            for (long ram: ramVals) {
                Main.logger.info("Running sketch with ram " + ram);
                System.out.println("Running sketch with ram " + ram);
                Main.logger.info("OMNISKETCH");
                System.out.println("OMNISKETCH");
                runSketch(ram, rtp);
            }

            System.gc(); // garbage collection
        }
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

    public void runSynopsisRamBased(Synopsis syn) throws IOException {
        long time_passed = 0;
        long start = System.currentTimeMillis();
        for (Record r : d.getOrderedDataset()) {
            syn.add(r);
        }
        long end = System.currentTimeMillis();
        time_passed = end - start;
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
        if (Main.datasetName.equals("CAIDA")) {
            ArrayList<Integer> order = new ArrayList<Integer>(Arrays.asList(7, 8, 5, 9, 4, 6));
            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = order.contains(j);
            }
            Main.filesToRead = Integer.parseInt(conditions[i]);
            Main.numFiles = Integer.parseInt(conditions[i]);}
        else if (Main.datasetName.equals("SNMP")) {
            ArrayList<Integer> order = new ArrayList<Integer>(Arrays.asList(12, 14, 11, 1, 5, 13, 9, 8, 10, 21, 15));
            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = false;
                attrsToUse[j] = order.contains(j);
            }
        }else {
            for (int j = 0; j < Main.numAttributes; j++) {
                attrsToUse[j] = true;
            }
        }
        d = new Dataset(Main.numQueries, Integer.MAX_VALUE, Main.datasetName, Main.numAttributes, attrsToUse, h);
    }

    public void readDatasetSettings() {
        switch (Main.datasetName) {
            case "CAIDA" -> {
                this.conditions = new String[]{"1", "5", "10", "15","20"}; //"1", "5", "10", "15", //, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 11;
            }
            case "SNMP" -> {
                this.conditions = new String[]{"0"};//, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 24;
            }
            case "synthzipf1-2" -> {
                this.conditions = new String[]{"1"};//, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 5;
            }
            default -> throw new RuntimeException("Unknown dataset name for scalability experiments");
        }
    }
}
