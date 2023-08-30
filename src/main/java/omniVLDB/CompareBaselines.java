package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.HYDRA_VLDB.HYDRA_VLDB.ImpHydraStruct;
import omniVLDB.omni.OmniSketch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CompareBaselines {
    // Class that compares the baselines
    Helper h;
    Dataset d;
    //ExpWorkload ew;
    RamToPar rtp;
    //int[] ramVals = {(int) (80*1E6), (int) (80*2E6), (int) (80*3E6), (int) (80*4E6), (int) (80*5E6),
     //       (int) (80*10E6), (int) (80*15E6), (int) (80*20E6)};//{5, 10, 50, 100, 150, 200, 300, 500, 750, 1000};// TODO: make sure these are okay ram vals
    long[] ramVals = { (long) (80*1E6), (long) (80*5E6), (long) (80*10E6),(long) (80*20E6)};
    String[] conditions;

    public CompareBaselines(Helper h) {
        this.h = h;

    }

    public void run() throws IOException, CsvValidationException {
        Main.useMultNumAttributes = true;
        readDatasetSettings();


        for (int i = 0; i < conditions.length; i++) {

            if (Main.useMultNumAttributes) {
                if (Main.datasetName.equals("CAIDA")) {
                    ArrayList<Integer> exp1 = new ArrayList<>(Arrays.asList(7, 10));
                    ArrayList<Integer> exp2 = new ArrayList<>(Arrays.asList(7, 10, 8));
                    ArrayList<Integer> exp3 = new ArrayList<>(Arrays.asList(7, 10, 8, 4));
                    ArrayList<Integer> exp4 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6));
                    ArrayList<Integer> exp5 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5));
                    ArrayList<Integer> exp6 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5, 9));
                    ArrayList<Integer> exp7 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5, 9, 3));
                    //ArrayList<Integer> exp8 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5, 9, 3, 2));
                    //ArrayList<Integer> exp9 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5, 9, 3, 2, 1));
                    //ArrayList<Integer> exp10 = new ArrayList<>(Arrays.asList(7, 10, 8, 4, 6, 5, 9, 3, 2, 1, 0));
                    ArrayList[] exps = new ArrayList[]{exp1, exp2, exp3, exp4, exp5, exp6, exp7};// exp8, exp9, exp10};

                    for (ArrayList exp : exps) {
                        System.out.println("Running experiment for " + exp.size() + " attributes");

                        attributesToRead = new boolean[Main.numAttributes];
                        for (int j = 0; j < Main.numAttributes; j++) {
                            attributesToRead[j] = false;
                            attributesToRead[j] = exp.contains(j);
                        }
                        readDataset(i);
                        runSyns(i);
                    }
                } else if (Main.datasetName.contains("synth")) {
                    ArrayList<Integer> exp1 = new ArrayList<>(Arrays.asList(0, 1));
                    ArrayList<Integer> exp2 = new ArrayList<>(Arrays.asList(0, 1, 2));
                    ArrayList<Integer> exp3 = new ArrayList<>(Arrays.asList(0, 1, 2, 3));
                    ArrayList<Integer> exp4 = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4));
                    ArrayList[] exps = new ArrayList[]{exp1, exp2, exp3, exp4};

                    for (ArrayList exp : exps) {
                        attributesToRead = new boolean[Main.numAttributes];
                        for (int j = 0; j < Main.numAttributes; j++) {
                            attributesToRead[j] = exp.contains(j);
                        }
                        readDataset(i);
                        runSyns(i);
                    }
                } else if (Main.datasetName.equals("SNMP")) {
                    int[] order = {12, 14, 11, 1, 5, 13, 9, 8, 10, 21, 15};
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
                        runSyns(i);
                    }
                }


            } else {

                attributesToRead = new boolean[Main.numAttributes];
                for (int j = 0; j < Main.numAttributes; j++) {
                    attributesToRead[j] = true;
                }
                readDataset(i);
                runSyns(i);
            }
        }
    }

    private void runSyns(int i) throws IOException {
        int numAttrsToUse = 0;
        for (boolean attr : attributesToRead) {
            if (attr)
                numAttrsToUse++;
        }

        Main.logger.info("Running dataset " + (i + 1) + " of " + conditions.length);
        System.out.println("Running dataset " + (i + 1) + " of " + conditions.length);
        this.rtp = new RamToPar(numAttrsToUse, ramVals);
        for (long ram : ramVals) {
            Main.logger.info("Running sketch with ram " + ram);
            System.out.println("Running sketch with ram " + ram);
            //Main.logger.info("OMNISKETCH");
            System.out.println("OMNISKETCH");
            runSketch(ram, rtp);
            //runCMBaseline(ram, rtp);
            //Main.logger.info("HYDRA");
            if (!Main.datasetName.contains("synth")) {
                System.out.println("HYDRA");
                runHydra(ram, rtp);
            }

            //System.out.println("Kmin baseline");
            //runKMinBaseline(ram, rtp);
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
        System.out.println("Number of elements added to sketch: " + hydra.totalAdded);
        System.out.println("Number of elements in stream:" + d.dataset.size());
        System.out.println("Number of indexed attributes: " + attrsIdx.size());
        System.out.println("Theoretical num elements added to sketch: " + d.dataset.size() * (Math.pow(2, attrsIdx.size()) - 1));

    }

    public void runSynopsisRamBased(Synopsis syn) throws IOException {
        int numUpdates = 0;
        long time_passed;
        if (Main.useWarmup) {
            time_passed = runSynWithWarmup(syn);
            numUpdates = d.dataset.size() - Main.warmupNumber;
        } else {
            runSynWithoutWarmup(syn);
            time_passed  = numUpdates = d.dataset.size();
        }
        //


//        long start = System.currentTimeMillis();
//
//
//        for (Record r : d.getOrderedDataset()) {
//            if (numUpdates % 1000000 == 0 && !Main.runOnODC) {
//                Main.logger.info("Number of updates: " + numUpdates);
//                System.out.println("Number of updates: " + numUpdates);
//            }
//            syn.add(r);
//
//            numUpdates++;
//        }
//        long end = System.currentTimeMillis();
        //time_passed = end - start;
        Main.logger.info("Time passed for updates synopsis " + syn.setting + " is: " + time_passed + " ms, average: " + (double) time_passed / numUpdates + " ms");
        Main.logger.info("Memory usage synopsis " + syn.setting + ": " + syn.getMemoryUsage());
        Main.logger.info("Memory usage dataset: " + d.getMemoryUsage());

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
        } else {
            // Convert conditions[i] to int
            Main.numFiles = Integer.parseInt(conditions[i]);
        }

        d = new Dataset(Main.numQueries, Integer.MAX_VALUE, Main.datasetName, Main.numAttributes, attributesToRead, h);
    }
    public boolean[] attributesToRead;//


    public void readDatasetSettings() {

        switch (Main.datasetName) {
            case "SNMP" -> {
                if (Main.useMultNumAttributes)
                    if (Main.runOnODC) {
                        this.conditions = new String[]{"0"};
                    } else {
                        this.conditions = new String[]{"03110_OR_03111"};//"03110_OR_031110_OR_031111_OR_031112_OR_031113"};//,
                    }

                else {
                    this.conditions = new String[]{"03110_OR_03111"};
                   /*"03110"/*,
                        "03110_OR_031110_OR_031111",
                        "03110_OR_031110_OR_031111_OR_031112_OR_031113",
                        "03110_OR_031110_OR_031111_OR_031112_OR_031113_OR_031114_OR_031115_OR_031116",
                        "03110_OR_03111"*/
                    ;
                }
                Main.numAttributes = 24;
            }
            case "CAIDA" -> {
                this.conditions = new String[]{Integer.toString(Main.filesToRead)};//, "4", "5", "6", "7", "8", "9", "10", "11", "12"};
                Main.numAttributes = 11;//10; //actually 7; can be 10;
            }

            // Want to be able to change which attributes are read instead of only count


            //Main.numFiles = 3;//conditions.length;
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
