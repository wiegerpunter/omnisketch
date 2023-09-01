package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.omni.OmniSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CompareDistributions {
    // Class that compares the baselines
    Helper h;
    Dataset d;
    //ExpWorkload ew;
    RamToPar rtp;
    long[] ramVals = { (long) (80*1E6), (long) (80*5E6), (long) (80*10E6),(long) (80*20E6)};
    String[] conditions;

    public CompareDistributions(Helper h) {
        this.h = h;

    }

    public void run() throws IOException, CsvValidationException {
        Main.sensitivityAnalysis = true;
        //String[] datasets = new String[]{"CAIDA", "SNMP", "synthzipf1-2", "synthzipf1-5","synthzipf2"};//"synth3zipf", "synthUniform", "synthDiffAlphas", "CAIDA", "SNMP"};
        String[] datasets = new String[]{"CAIDA", "SNMP", "synthzipf1-3", "synthzipf1-5", "synthzipf1", "synthUniform"};
        Main.createNewWorkload = true;
        for (String name : datasets) {
            System.out.println("Running dataset " + name);
            Main.datasetName = name;
            h.initFileWriter();
            readDatasetSettings();

            for (int i = 0; i < conditions.length; i++) {

                attributesToRead = new boolean[Main.numAttributes];
                for (int j = 0; j < Main.numAttributes; j++) {
                    attributesToRead[j] = true;
                }
                readDataset(i);
                runSyns(i);
            }
        }

        // Structure:
        // 1. Run sketch with different RAM values
        // 2. For all RAM values, run the baselines based on ram value. after evaluation of one sketch delete sketch.
        // 3. In every evaluation, save the results in a file.
    }

    private void runSyns(int i) throws IOException {
        int numAttrsToUse = 5;
        this.rtp = new RamToPar(numAttrsToUse, ramVals);
        for (long ram : ramVals) {
            Main.logger.info("Running sketch with ram " + ram);
            System.out.println("Running sketch with ram " + ram);
            //Main.logger.info("OMNISKETCH");
            System.out.println("OMNISKETCH");
            runSketch(ram, rtp);
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
        int numUpdates = 0;
        long time_passed;
        if (Main.useWarmup) {
            time_passed = runSynWithWarmup(syn);
            numUpdates = Main.sensitivityNumberOfRecords;
        } else {
            runSynWithoutWarmup(syn);
            time_passed  = numUpdates = Main.sensitivityNumberOfRecords + Main.warmupNumber;
        }
        System.out.println("Time passed for updates synopsis " + syn.setting + " is: " + time_passed + " ms, average: " + (double) time_passed / numUpdates + " ms");
        System.out.println("Memory usage synopsis " + syn.setting + ": " + syn.getMemoryUsage());
        System.out.println("Memory usage dataset: " + d.getMemoryUsage());
        System.out.println("\n");
        AnalysisBaselines ab = new AnalysisBaselines(syn, d, h, time_passed);
        ab.run();
        syn.reset();
        //ConditionChecks.run(d, s);
    }

    private long runSynWithoutWarmup(Synopsis syn) {
        long time_passed = 0;
        System.out.println("Running synopsis");
        int numUpdates = 0;
        long startTime = System.currentTimeMillis();

        for (Record r: d.dataset) {
            if (numUpdates % 1000000 == 0 && !Main.runOnODC) {
                Main.logger.info("Number of updates: " + numUpdates);
                System.out.println("Number of updates: " + numUpdates);
            }
            syn.add(r);
            numUpdates++;
        }

        // Stop the timer
        long endTime = System.currentTimeMillis();
        time_passed = endTime - startTime;
        return time_passed;
    }

    private long runSynWithWarmup(Synopsis syn) {
        int numUpdates = 0;
        long time_passed = 0;

        System.out.println("Running synopsis");
        for (Record r: d.warmupDataset) {
            if (numUpdates % 1000000 == 0 && !Main.runOnODC) {
                System.out.println("Number of updates: " + numUpdates);
            }
            //System.out.println("Adding record " + i);
            //System.out.println(d.getOrderedDataset().get(i));
            syn.add(r);
            numUpdates++;
        }
        // Start the timer
        numUpdates = 0;
        long startTime = System.currentTimeMillis();
        for (Record r: d.ingestionDataset) {
            if (numUpdates >= Main.sensitivityNumberOfRecords) {
                break;
            }
            if (numUpdates % 1000000== 0 && !Main.runOnODC) {
                System.out.println("Number of updates: " + numUpdates);
            }
            syn.add(r);
            numUpdates++;
        }

        // Stop the timer
        long endTime = System.currentTimeMillis();
        time_passed = endTime - startTime;

        return time_passed;
    }

    public void readDataset(int i) throws IOException, CsvValidationException {
        if (Main.datasetName.equals("SNMP")) {
            Main.fileStartCondition = conditions[i];
            ArrayList<Integer> order = new ArrayList<>(Arrays.asList(12, 14, 11, 1, 5));
            attributesToRead = new boolean[Main.numAttributes];
            for (int j = 0; j < Main.numAttributes; j++) {
                attributesToRead[j] = order.contains(j);
            }
        } else if (Main.datasetName.equals("CAIDA")) {
            // Convert conditions[i] to int
            if (!Main.createNewWorkload) {
                ArrayList<Integer> order = new ArrayList<Integer>(Arrays.asList(7, 8, 5,9, 4));

                attributesToRead = new boolean[Main.numAttributes];
                for (int j = 0; j < Main.numAttributes; j++) {
                    attributesToRead[j] = order.contains(j);
                }
            }
            Main.numFiles = Integer.parseInt(conditions[i]);
        } else {
            if (!Main.datasetName.contains("synth")) {
                throw new RuntimeException("Unknown dataset name");
            }
            attributesToRead = new boolean[Main.numAttributes];
            for (int j = 0; j < Main.numAttributes; j++) {
                attributesToRead[j] = true;
            }
        }

        d = new Dataset(Main.numQueries, Integer.MAX_VALUE, Main.datasetName, Main.numAttributes, attributesToRead, h);
    }
    public boolean[] attributesToRead;//


    public void readDatasetSettings() {

        switch (Main.datasetName) {
            case "SNMP" -> {
                this.conditions = new String[]{"0"};
                Main.numAttributes = 24;
            }
            case "CAIDA" -> {
                this.conditions = new String[]{Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 11;//10; //actually 7; can be 10;
            }
            default -> {
                if (!Main.datasetName.contains("synth")) {
                    throw new RuntimeException("Unknown dataset name");
                }
                Main.numAttributes = 5;
                this.conditions = new String[]{Integer.toString(Main.filesToRead)};
            }
        }
    }
}
