package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.omni.OmniSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class RangeQueries {
    // Class that does experiments for range queries
    Helper h;
    Dataset d;
    int[] BVals = {500, 1000, 1500, 2000, 2500};
    String[] conditions;
    public boolean[] attributesToRead;//
    public RangeQueries(Helper h) {
        this.h = h;

    }

    public void run() throws IOException, CsvValidationException {
        Main.rangeQueries = true;
        h.initFileWriter();

        readDatasetSettings();
        Main.eps = 0.1;
        Main.delta = 0.1;

        for (int i = 0; i < conditions.length; i++) {
            int[] order = {12, 14, 15, 13, 8, 9,10, 11};
            ArrayList[] exps = new ArrayList[order.length - 1];
            for (int d= 1; d< order.length; d++) {
                ArrayList<Integer> temp = new ArrayList<>();
                for (int j = 0; j <= d; j++) {
                    temp.add(order[j]);
                }
                exps[d - 1] = temp;
            }

            for (ArrayList<Integer> exp : exps) {
                System.out.println("Running experiment for " + exp.size() + " attributes");
                attributesToRead = new boolean[Main.numAttributes];
                for (int j = 0; j < Main.numAttributes; j++) {
                    attributesToRead[j] = exp.contains(j);
                }
                readDataset(i);
                Main.logger.info("Running dataset " + (i + 1) + " of " + conditions.length);
                System.out.println("Running dataset " + (i + 1) + " of " + conditions.length);
                for (int B: BVals) {
                    Main.maxSize = B;
                    Main.b = (int) Math.ceil(Math.log(4*Math.pow(B, (double) 5/2)/Main.delta));
                    Main.width = 20;
                    Main.depth = 3;
                    runSketch();
                }
            }
        }
    }

    public void runSketch() throws IOException {
        Main.s = new OmniSketch(d.attributesInWorkload);
        runSynopsisRamBased(Main.s);
    }

    public void runSynopsisRamBased(Synopsis syn) throws IOException {
        long time_passed = 0;
        long start = System.currentTimeMillis();
        int i = 0;
//
//        // First get cardinality of each attribute and print it before running the synopsis
        int[] maxBits = new int[Main.numAttributes];
        for (int j = 0; j < d.attributesInWorkload.length; j++) {
            if (d.attributesInWorkload[j]) {
                // Get cardinality
                maxBits[j] = d.getCardinality(j);
            }
        }
        syn.maxBits = maxBits;

//

        for (Record r : d.getOrderedDataset()) {
            // Every 1000 records, print progress
            if (i % 100000 == 0) {
                System.out.println("Progress: " + i + " of " + d.size);
            }
            i++;

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
            attrsToUse = attributesToRead;
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
                this.conditions = new String[]{"1", "5", "10", "15", "20"}; //"1", "5", "10", "15", //, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 11;
            }
            case "SNMP" -> {
                this.conditions = new String[]{"0"};//, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 24;
            }
            case "synthzipf1-2" -> {
                this.conditions = new String[]{"1"};//, "10", "15", "20"//Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 5; //actually 7; can be 10;
            }
            default -> throw new RuntimeException("Unknown dataset name for scalability experiments");
        }
    }
}
