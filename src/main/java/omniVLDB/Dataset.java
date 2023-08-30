package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import omniVLDB.dataGeneration.ProcessedStreamLoaderGeneric;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


public class Dataset implements Externalizable{
    //OmniSketch s;
    int size;
    String setting;
    public ArrayList<Record> dataset = new ArrayList<>();
    public ArrayList<Record> warmupDataset = new ArrayList<>();
    public ArrayList<Record> ingestionDataset = new ArrayList<>();
    ArrayList<Query> queries = new ArrayList<>();
    ArrayList<Query> altQueries = new ArrayList<>();
    private int totalRead;
    ArrayList<Integer> usedIDs = new ArrayList<>();
    Helper h;
    int numZipfAttrs;
    double zipfAlpha;
    boolean[] attrsToRead;

    public Dataset(int numQueries, int range, String setting, int numAttrsToUse, boolean[] attrsToRead, Helper h) throws IOException, CsvValidationException {
        //this.s = s;
        this.h = h;
        this.setting = setting;
        this.attrsToRead = attrsToRead;
        switch (setting) {
            case "SNMP" -> {
                Main.numAttributes = numAttrsToUse;

                // Check if file exists
                String CSV_FILE_NAME = Main.outputFolder + "SNMPDataset_" + Main.datasetName + "_"+ Main.fileStartCondition + ".csv";
                File f = new File(CSV_FILE_NAME);

                if (!f.exists()) {
                    SNMPDataset();
                    Main.writeSNMP = false;
                } else {
                    System.out.println("Loading dataset from file, name: " + CSV_FILE_NAME);
                    dataset = Main.h.readSNMPDataset();
                    System.out.println("Loaded size: " + dataset.size());
                }

                if (Main.rangeQueries) {
                    if (Main.createNewRangeWorkload) {
                        AltWorkloadRange();
                        Main.createNewRangeWorkload = false;
                    } else {
                        loadRangeWorkload();
                    }
                } else {
                    if (Main.createNewWorkload) {
                        pointQueryWorkload();
                        Main.createNewWorkload = false;
                    } else {
                        loadWorkload();
                    }
                }
            }
            case "CAIDA" -> {
                Main.numAttributes = numAttrsToUse;
                CAIDADataset();
                System.out.println("CAIDA needs to create new workload yes/no? " + Main.createNewWorkload);
                if (Main.createNewWorkload) {
                    pointQueryWorkload();
                } else {
                    loadWorkload();
                }
            }
            case "synthzipf1" -> {
                zipfDataset(numAttrsToUse, 5, 1);
            } case "synthzipf1-3" -> {
                zipfDataset(numAttrsToUse, 5, 1.3);
            }
            case "synthzipf1-5" -> {
                zipfDataset(numAttrsToUse, 5, 1.5);
            }
            case "synthUniform" -> {
                zipfDataset(numAttrsToUse, 0, 0); // Uniform distribution
            }
            default -> {
                Main.logger.severe("Invalid setting");
                System.out.println("Invalid setting");
                System.exit(0);
            }
        }
        this.size = dataset.size();
        dataset.sort(Comparator.comparingLong(o -> o.timestamp));

        warmupDataset = new ArrayList<>(dataset.subList(0, Main.warmupNumber));
        ingestionDataset = new ArrayList<>(dataset.subList(Main.warmupNumber, dataset.size()));

        System.out.println("Dataset loaded with size: " + dataset.size() + " and "+ queries.size() + " queries");
    }

    public void zipfDataset(int numAttrsToUse, int numZipfAttrs, double zipfAlpha) throws CsvValidationException, IOException {
        Main.numAttributes = numAttrsToUse;
        numZipfAttrs = 5;
        zipfAlpha = 1.5; // Zipf distribution parameter
        boolean differAlphas = false;
        String CSV_FILE_NAME = Main.outputFolder + "synthDataset_" + Main.datasetName + "_N="+ Main.sensitivityNumberOfRecords +
                "_ZipfAttrs="+ numZipfAttrs + "_zipfAlpha="+ zipfAlpha + "_differAlphas="+ differAlphas + ".csv";
        File f = new File(CSV_FILE_NAME);
        if (f.exists()) {
            dataset = h.readSynthDataset(Main.sensitivityNumberOfRecords, numZipfAttrs, zipfAlpha, false);
        } else {
            synthDataset(Main.sensitivityNumberOfRecords, numZipfAttrs, zipfAlpha, false);
        }
        if (Main.createNewWorkload) {
            synthWorkload();
            Main.createNewWorkload = false;
        } else {
            loadWorkload();
        }

        if (Main.rangeQueries) {
            if (Main.createNewRangeWorkload) {
                rangeWorkload();
                Main.createNewRangeWorkload = false;
            } else {
                loadRangeWorkload();
            }
        }

    }

    private void loadWorkload() throws IOException, CsvValidationException {
        System.out.println("Loading workload from file, dataset has size " + dataset.size());
        Main.h.readWorkload(dataset.size(), this);

        for (Query q: queries) {
            ArrayList<Integer> loopList = new ArrayList<>(q.predAttrs);
            for (int attr : loopList) {
                q.pointAttrs[attr] = true;
                attributesInWorkload[attr] = true;
            }
        }
        System.out.println("Loaded " + queries.size() + " queries from workload");

        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }

    }

    private void loadRangeWorkload() throws IOException, CsvValidationException {
        System.out.println("Loading workload from file, dataset has size " + dataset.size());
        Main.h.readRangeWorkload(dataset.size(), this);

        for (Query q: rangeQueries) {
            ArrayList<Integer> loopList = new ArrayList<>(q.predAttrs);
            for (int attr : loopList) {
                //q.rangeAttrs.set(attr, true);
                attributesInWorkload[attr] = true;
            }
        }


        Main.logger.info("Loaded " + queries.size() + " queries from workload");
        System.out.println("Loaded " + queries.size() + " queries from workload");

        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }
    }



    void batchExact(long[] rec, ArrayList<Query> queries) {
        boolean satisfies;
        for (Query q : queries) {
            satisfies = true;
            for (int attr : q.predAttrs) {
                if (q.pointAttrs[attr]) {
                    if (rec[attr] != q.record[attr]) {
                        satisfies = false;
                        break;
                    }
                }
            }
            if (satisfies) {
                q.exactAnswer++;
            }
    }
    }

    public Dataset() {
    }

    public Dataset(Dataset d) {
        this.size = d.size;
        this.setting = d.setting;
        this.dataset = d.dataset;
        this.queries = d.queries;
        this.totalRead = d.totalRead;
        this.totalSkip = d.totalSkip;
        this.queryWithPredicates = d.queryWithPredicates;
        this.distinctPredSize = d.distinctPredSize;
        this.queryId = d.queryId;
        this.dStats = null;
        this.recId = d.recId;
        this.usedIDs = d.usedIDs;
        this.attributesInWorkload = d.attributesInWorkload;
    }
    public ArrayList<Record> getOrderedDataset() {
        //Sort records in dataset by timestamp
        dataset.sort(Comparator.comparingLong(o -> o.timestamp));
        return dataset;
    }
    private int totalSkip = 0;

    int recId = 0;


    public void CAIDADataset() throws RuntimeException {
        String dir;

        loadCAIDAFile(Main.readFolder + "/CAIDA/csv/");
    }

    private void loadCAIDAFile(String path) throws RuntimeException {
        File dir = new File(path);//File subDir = new File(directory + child);
        File[] filesList = dir.listFiles();

        //int id = 0;
        int filesSeen = 0;
        if (filesList != null) {
            Arrays.sort(filesList, (f1, f2) -> f1.getName().compareToIgnoreCase(f2.getName()));
            for (File child : filesList) {
                if (child.getName().endsWith(".pcap.csv")) {
                    Main.logger.info("Reading file: " + child.getName());
                    System.out.println(child.getName());
                    ProcessedStreamLoaderGeneric psl = new ProcessedStreamLoaderGeneric(child.getPath(), false);
                    psl.reset();
                    int cnt = 0;

                    String firstLine = psl.readFirst();
                    String nextLine = psl.readRecord(firstLine, recId, dataset);
                    recId++;

                    while (!(nextLine == null)) {
                        nextLine = psl.readRecord(nextLine, recId, dataset); // Returns null at end of file.
                        recId++;
                        cnt++;
                        if (cnt == 9999999) {
                            throw new RuntimeException("Too many records");
                        }
                    }
                    this.totalSkip += psl.skip; //Number of records skipped.
                    this.totalRead += psl.linesSeen;
                    psl.close();
                    filesSeen++;
                    if (filesSeen >= Main.numFiles) {
                        break;
                    }
                    if (!Main.readAllFiles) {
                        break;
                    }
                }
            }
        }
    }



    public void SNMPDataset() throws RuntimeException, IOException {
        if (Main.readAllFiles) {
            //String directory = "C:/Users/s162378/OneDrive - TU Eindhoven/Documents/GitHub/DSCM/fall03.tar/fall03/fall03/";
            String directory = Main.readFolder + "/SNMP/";
            File dir = new File(directory);

            String[] directoryListing = dir.list((current, name) -> new File(current, name).isDirectory());

            if (directoryListing != null) {
                for (String child : directoryListing) {
                    String path = directory + child;
                    if (Main.fileStartCondition.contains("_OR_")) {
                        String[] conditions = Main.fileStartCondition.split("_OR_");
                        boolean skip = true;
                        for (String condition : conditions) {
                            if (child.startsWith(condition)) {
                                skip = false;
                                break;
                            }
                        }
                        if (skip) {
                            continue;
                        }
                    } else if (!child.startsWith(Main.fileStartCondition)) {
                        continue;
                    }
                    loadFileSNMP(path);
                }
            } else {
                throw new RuntimeException("No directories found");
            }
            Main.logger.info("All files read.");
            System.out.println("All files read.");
        } else {
            String smallPath = Main.readFolder + "/SNMP/031101/";
            loadFileSNMP(smallPath);
        }


        //Check for whole dataset how many records have value -999 per attribute:
        int[] count = new int[Main.numAttributes];
        for (Record r : dataset) {
            for (int i = 0; i < Main.numAttributes; i++) {
                if (r.getRecord()[i] == -999) {
                    count[i]++;
                }
            }
        }
        for (int i = 0; i < Main.numAttributes; i++) {
            System.out.println("Attribute " + i + " has " + count[i] + " records with value -999");
        }


        ArrayList<Record> validRecords = new ArrayList<>();
        boolean validRecord;
        HashSet<Integer> invalidIndices = new HashSet<>(Arrays.asList(3, 16, 17, 18, 19, 20));

        for (Record record : dataset) {
            validRecord = true;

            long[] recordData = record.getRecord();
            for (int i = 0; i < Main.numAttributes; i++) {
                // if i is not 3, 16, 17, 18, 19, 20.
                if (!invalidIndices.contains(i) && recordData[i] == -999) {
                    validRecord = false;
                    break;
                }
            }
            if (validRecord) {
                validRecords.add(record);
            }
        }


// Replace the original dataset with the new validRecords ArrayList
        dataset = validRecords;
        // delete records with value -999
//        for (int i = 0; i < Main.numAttributes; i++) {
//            // if i is not 3, 16, 17, 18, 19, 20.
//            if (i != 3 && i != 16 && i != 17 && i != 18 && i != 19 && i != 20) {
//                for (int j = 0; j < dataset.size(); j++) {
//                    if (dataset.get(j).getRecord()[i] == -999) {
//                        dataset.remove(j);
//                        j--;
//                    }
//                }
//            }
//        }

        // write dataset to file
        h.initSNMPDataset(this);
        for (Record r : dataset) {
            h.writeSNMPDataset(((RecordSNMP) r).writeRecord());
        }



        Main.logger.info("Skipped " + totalSkip + " records");
        Main.logger.info("Read " + totalRead + " records");
        Main.logger.info("Size: " + dataset.size());
        System.out.println("Skipped " + totalSkip + " records");
        System.out.println("Read " + totalRead + " records");
        System.out.println("Size: " + dataset.size());

    }


    private void loadFileSNMP(String path) {
        File dir = new File(path);//File subDir = new File(directory + child);
        File[] filesList = dir.listFiles();
        //int id = 0;
        if (filesList != null) {
            for (File child : filesList) {
                if (child.getName().endsWith(".snmp.gz")) {
                    //System.out.println("Processing " + child.getPath());
                    ProcessedStreamLoaderGeneric psl = new ProcessedStreamLoaderGeneric(child.getPath(), true);
                    psl.reset();
                    //psl.readHeader();

                    int cnt = 0;

                    String firstLine = psl.readFirst();
                    String nextLine = psl.readRecord(firstLine, recId, dataset);
                    recId++;

                    while (!(nextLine == null)) {
                        nextLine = psl.readRecord(nextLine, recId, dataset); // Returns null at end of file.
                        recId++;
                        cnt++;
                        if (cnt == 999999) {
                            throw new RuntimeException("Too many records");
                        }
                    }
                    this.totalSkip += psl.skip; //Number of records skipped.
                    this.totalRead += psl.linesSeen;
                psl.close();
                }
            }
        }
    }
    AttributeStats[] dStats;

    int queryId = 0;
    boolean[] queryWithPredicates = new boolean[Main.numAttributes];

    boolean[] attributesInWorkload = new boolean[Main.numAttributes];
    int distinctPredSize;


    LinkedHashMap<Integer, LinkedHashMap<Integer, ArrayList<Integer>>> perPredMap;
    ArrayList<Integer> attrsIdx;
    int sampledRecords = 0;
    int subPopulations = 0;
    int genSubQueries = 0;
    int droppedBecauseDuplicate = 0;
    int droppedBecauseNull = 0;
    int droppedBecauseInvalidAttr = 0;
    int droppedBecauseOnePred = 0;

    public void pointQueryWorkload() throws IOException {
        h.initWorkloadFileWriter(dataset.size());
        System.out.println("Creating workload");
        SubPopMap subPopMap = new SubPopMap(Main.numAttributes);
        queryWithPredicates = new boolean[Main.numAttributes];
        this.dStats = new DatasetStats(this).computeStats();
        System.out.println("Dataset stats computed");
        perPredMap = subPopMap.perPredMap;
        attrsIdx = subPopMap.attrsIdx;
         // For loop over all records in dataset
        // 1) select randomly a record.
        // 2) select randomly a subset of attributes
        // 3) make query with those attributes
        // 4) add to altQueries with count 0
        // After for loop, new for loop over all records in dataset
        // for every record, check for every query whether it satisfies predicate
        // If so, increase count by 1 of that query
        Random rn;

        ArrayList<Integer> attrs;
        for (int i = 0; i < dataset.size(); i++) {
            rn = new Random(i);
            if (i % 100000 == 0) {
                System.out.println("Generating pot queries: " + i);
            }
            if (rn.nextInt(5000) != 0) {
                continue;
            }
            sampledRecords++;
            long[] record = dataset.get(i).record;

            for (int j = 1; j < attrsIdx.size(); j++){
                //int j = attrsIdx.size();
                int numRandomSubPops = 10;
                List<Integer> randomSubPops = getRandomKeys(perPredMap.get(j), i, numRandomSubPops);
                for (int subpopID : randomSubPops) {
                    subPopulations++;
                    boolean validQuery = true;
                    attrs = perPredMap.get(j).get(subpopID);
                    // 3) make query with those attributes
                    StringBuilder queryString = new StringBuilder();
                    for (int attr = 0; attr < Main.numAttributes; attr++) {
                        if (attrs.contains(attr)) {
                            AttributeStats a = dStats[attr];
                            if (a.dontIncludeAttribute(new StringBuilder(queryString))) {
                                validQuery = false;
                                droppedBecauseInvalidAttr++;
                                break;
                            }
                            long value = record[attr];
                            if (value == -999) {
                                validQuery = false;
                                droppedBecauseNull++;
                                break;
                            }
                            if (queryString.toString().equals("")) {
                                queryString.append(a.attributeName).append(" = ").append(value);
                            } else {
                                queryString.append(" and ").append(a.attributeName).append(" = ").append(value);
                            }
                        }
                    }
                    if (!validQuery) {
                        continue;
                    }
                    Query q = new Parser(queryId).parse(queryString.toString());
                    queryId++;
                    for (Query q2 : altQueries) {
                        if (q2.pointQueryEqual(q)) {
                            validQuery = false;
                            break;
                        }
                    }
                    if (!validQuery) {
                        droppedBecauseDuplicate++;
                        continue;
                    }
                    if (q.predAttrs.size() == 1) {
                        droppedBecauseOnePred++;
                        continue;
                    }
                    // 4) add to altQueries with count 0
                    q.exactAnswer = 0;
                    altQueries.add(q);
                    addSubQueries(q, record, attrs);
                    for (int attr : q.predAttrs) {
                        attributesInWorkload[attr] = true;
                    }
                    queryWithPredicates[q.predAttrs.size() - 1] = true;
                }

            }
        }

        // Give overview of counters
        System.out.println("Sampled records: " + sampledRecords + " out of " + dataset.size());
        System.out.println("Potential base queries " + subPopulations);
        System.out.println("Generated subqueries: " + genSubQueries);
        System.out.println("Dropped because duplicate: " + droppedBecauseDuplicate);
        System.out.println("Dropped because null: " + droppedBecauseNull);
        System.out.println("Dropped because invalid attr: " + droppedBecauseInvalidAttr);
        System.out.println("Dropped because one pred: " + droppedBecauseOnePred);
        System.out.println("Number of queries to check: " + altQueries.size());


        Main.logger.info("Created " + altQueries.size() + " queries");
        System.out.println("Number of queries to check: " + altQueries.size());

        // After for loop, new for loop over all records in dataset
        double maxExactAnswer = 0;
        long startTime1 = System.currentTimeMillis();
        boolean satisfies;
        long[] rec;
        for (int i = 0; i < dataset.size(); i++) {
            if (i % 10000 == 0) {
                System.out.println("Processing record " + i);
            }

            rec = dataset.get(i).getRecord();
            // for every record, check for every query whether it satisfies predicate

            batchExact(rec, altQueries);
        }
        for (Query q : altQueries) {
            if (q.exactAnswer > maxExactAnswer) {
                maxExactAnswer = q.exactAnswer;
            }
        }
        long endTime = System.currentTimeMillis();
        Main.logger.info("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altQueries.size() + " ms per query");
        System.out.println("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altQueries.size() + " ms per query");
        System.out.println("Max exact answer: " + maxExactAnswer);
        int binSize = 1000;

        // Step 1: Calculate the quartiles
        List<Double> exactAnswers = new ArrayList<>();
        for (Query q : altQueries) {
            exactAnswers.add(q.exactAnswer);
        }

        Collections.sort(exactAnswers);
        int n = exactAnswers.size();
        double Q1 = exactAnswers.get(n / 4);
        double Q2 = exactAnswers.get(n / 2);
        double Q3 = exactAnswers.get((3 * n) / 4);
        System.out.println("Q1: " + Q1);
        System.out.println("Q2: " + Q2);
        System.out.println("Q3: " + Q3);
        System.out.println("Max exact answer: " + maxExactAnswer);

// Step 2: Assign queries to their corresponding quartile bin
        int[] queryBins = new int[4];
        int droppedQueriesQ1 = 0;
        int droppedQueriesQ2 = 0;
        int droppedQueriesQ3 = 0;
        int droppedQueriesQ4 = 0;
        for (Query q : altQueries) {
            if (q.exactAnswer < Q1) {
                q.queryBin = 0;
                queryBins[0]++;
                queries.add(q);
            } else if (q.exactAnswer >= Q1 && q.exactAnswer < Q2) {
                q.queryBin = 1;
                queryBins[1]++;

                queries.add(q);
            } else if (q.exactAnswer >= Q2 && q.exactAnswer < Q3) {
                q.queryBin = 2;
                queries.add(q);
            } else {
                q.queryBin = 3;
                queries.add(q);
                queryBins[3]++;
            }
        }
        System.out.println("Dropped queries Q1: " + droppedQueriesQ1);
        System.out.println("Dropped queries Q2: " + droppedQueriesQ2);
        System.out.println("Dropped queries Q3: " + droppedQueriesQ3);
        System.out.println("Dropped queries Q4: " + droppedQueriesQ4);
        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }
        for (Query q: queries) {
            Main.h.writeQuery(q, dataset.size());
        }


    }
    
    
    

    public void addSubQueries(Query q, long[] record, List<Integer> attrs) {
        if (q.predAttrs.size() == 2) {
            return;
        }
        int numPredicates = q.predAttrs.size();
        for (int i = 0; i < numPredicates; i++) {
            List<Integer> subQueryAttrs = new ArrayList<>(attrs);
            subQueryAttrs.remove(q.predAttrs.get(i)); // Remove one predicate at a time
            boolean validQuery = true;
            // 3) make query with those attributes
            StringBuilder queryString = new StringBuilder();
            for (int attr = 0; attr < Main.numAttributes; attr++) {
                if (subQueryAttrs.contains(attr)) {
                    AttributeStats a = dStats[attr];
                    if (a.dontIncludeAttribute(new StringBuilder(queryString))) {
                        validQuery = false;
                        break;
                    }
                    long value = record[attr];
                    if (value == -999) {
                        validQuery = false;
                        break;
                    }
                    if (queryString.toString().equals("")) {
                        queryString.append(a.attributeName).append(" = ").append(value);
                    } else {
                        queryString.append(" and ").append(a.attributeName).append(" = ").append(value);
                    }
                }
            }
            if (!validQuery) {
                continue;
            }
            Query subQ = new Parser(queryId).parse(queryString.toString());
            queryId++;
            for (Query q2 : altQueries) {
                if (q2.pointQueryEqual(subQ)) {
                    validQuery = false;
                    break;
                }
            }
            if (!validQuery) {
                continue;
            }
            if (subQ.predAttrs.size() == 1) {
                continue;
            }
            // 4) add to altQueries with count 0
            subQ.exactAnswer = 0;
            altQueries.add(subQ);
            genSubQueries++;
            addSubQueries(subQ, record, subQueryAttrs);
            for (int attr : subQ.predAttrs) {
                attributesInWorkload[attr] = true;
            }
            queryWithPredicates[subQ.predAttrs.size() - 1] = true;
        }
    }
    long[][] minMaxValues = new long[Main.numAttributes][2];

    public void AltWorkloadRange() throws IOException {
        h.initRangeWorkloadFileWriter(dataset.size());
        int maxCombinations = (int) Math.pow(2, Main.numAttributes) -1;
        Main.logger.info("Creating workload");
        System.out.println("Creating workload");
        SubPopMap subPopMap = new SubPopMap(Main.numAttributes);
        queryWithPredicates = new boolean[Main.numAttributes];
        this.dStats = new DatasetStats(this).computeStats();
        System.out.println("Dataset stats computed");
        perPredMap = subPopMap.perPredMap;
        attrsIdx = subPopMap.attrsIdx;


        for (int i = 0; i < Main.numAttributes; i++) {
            minMaxValues[i][0] = Long.MAX_VALUE;
            minMaxValues[i][1] = Long.MIN_VALUE;
        }
        for (Record value : dataset) {
            long[] record = value.record;
            for (int attr = 0; attr < Main.numAttributes - 2; attr++) {
                if (record[attr] < minMaxValues[attr][0] && record[attr] != -999) {
                    minMaxValues[attr][0] = record[attr];
                }
                if (record[attr] > minMaxValues[attr][1] && record[attr] != -999) {
                    minMaxValues[attr][1] = record[attr];
                }
            }
        }

        //Print min max values and range
        for (int attr = 0; attr < Main.numAttributes - 2; attr++) {
            System.out.println("Attribute: " + attr + " Min: " + minMaxValues[attr][0] + " Max: " + minMaxValues[attr][1] + " Range: " + (minMaxValues[attr][1] - minMaxValues[attr][0]));
        }

        Random rn;


        int[] subPopPerPred = new int[attrsIdx.size()];
        ArrayList<Integer> attrs;

        // Pick two attributes out of the good attributes
        int numToGenerate = 1000;
        int[] rangeSizes = new int[]{(int) Math.pow(2,10), (int) Math.pow(2,15), (int) Math.pow(2, 20), (int) Math.pow(2, 25)};
        int[] sizesOfp = new int[]{2, 3, 4};

        for (int j = 0; j < numToGenerate; j++) {
            Random r = new Random(j);
            for (int p: sizesOfp) { // For each size of p
                for (int l: rangeSizes) { // For each range size
                    ArrayList<Integer> attrsToGen = new ArrayList<>(); // Attributes to generate
                    while (attrsToGen.size() < p) { // Pick p attributes
                        int attr = r.nextInt(Main.numAttributes); // Pick an attribute
                        if (!attrsToGen.contains(attr)) { // If we haven't picked it before
                            if (attrsToRead[attr]) { // If it's an attribute we want to read
                                if (Main.datasetName.equals("SNMP")) {
                                    if (Parser.ordinalAttributesSNMP.contains(Parser.attributeNamesSNMP[attr])){ // If it's an ordinal attribute
                                        attrsToGen.add(attr);
                                    }
                                } else if (Main.datasetName.equals("CAIDA")) {
                                    if (Parser.ordinalAttributesCAIDA.contains(Parser.attributeNamesCAIDA[attr])){
                                        attrsToGen.add(attr);
                                    }
                                } else {
                                    attrsToGen.add(attr);
                                }
                            }
                        }
                    }

                    StringBuilder queryString = new StringBuilder();
                    for (Integer i : attrsToGen) {
                        long low = minMaxValues[i][0];
                        long high = minMaxValues[i][1];
                        if (high - low < l) {
                            //System.out.println("Range too big for attribute " + Parser.getAttributeName(i) + " with range " + l + " and min " + low + " and max " + high);
                            continue;
                        }
                        long randomStartPoint = r.nextLong(low, high); // Generate random number in range [low, high]
                        long endPoint = randomStartPoint + l;
                        if (queryString.toString().equals("")) {
                            queryString.append(Parser.getAttributeName(i)).append(" in ").append(randomStartPoint).append(",").append(endPoint);
                        } else {
                            queryString.append(" and ").append(Parser.getAttributeName(i)).append(" in ").append(randomStartPoint).append(",").append(endPoint);
                        }
                    }
                    if (queryString.toString().equals("")) {
                        continue;
                    }
                    Query q = new Parser(queryId).parse(queryString.toString());
                    queryId++;
                    q.exactAnswer = 0;
                    boolean validQuery = true;
                    for (Query q2 : altRangeQueries) {
                        if (q2.rangeQueryEqual(q)) {
                            validQuery = false;
                            break;
                        }
                    }
                    if (!validQuery) {
                        continue;
                    }
                    altRangeQueries.add(q);
                    rangeSubQueries(q, attrsIdx);
                    for (int attr : q.predAttrs) {
                        attributesInWorkload[attr] = true;
                    }
                    queryWithPredicates[q.predAttrs.size() - 1] = true;
                }
            }
        }

        // Give overview of counters
        System.out.println("Sampled records: " + sampledRecords + " out of " + dataset.size());
        System.out.println("Potential base queries " + subPopulations);
        System.out.println("Generated subqueries: " + genSubQueries);
        System.out.println("Dropped because duplicate: " + droppedBecauseDuplicate);
        System.out.println("Dropped because null: " + droppedBecauseNull);
        System.out.println("Dropped because invalid attr: " + droppedBecauseInvalidAttr);
        System.out.println("Dropped because one pred: " + droppedBecauseOnePred);
        System.out.println("Number of queries to check: " + altRangeQueries.size());

        // After for loop, new for loop over all records in dataset
        double maxExactAnswer = 0;
        long startTime1 = System.currentTimeMillis();
        boolean satisfies;
        long[] rec;
        for (int i = 0; i < dataset.size(); i++) {
            if (i % 10000 == 0) {
                System.out.println("Processing record " + i);
            }

            rec = dataset.get(i).getRecord();
            // for every record, check for every query whether it satisfies predicate

            batchExactRange(rec, altRangeQueries);
        }

        long endTime = System.currentTimeMillis();
        for (Query q : altRangeQueries) {
            if (q.exactAnswer > 0) {
                rangeQueries.add(q);
            }
            if (q.exactAnswer > maxExactAnswer) {
                maxExactAnswer = q.exactAnswer;
            }
        }

        System.out.println("Accepted " + rangeQueries.size() + " out of " + altRangeQueries.size());
        Main.logger.info("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altRangeQueries.size() + " ms per query");
        System.out.println("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altRangeQueries.size() + " ms per query");
        System.out.println("Max exact answer: " + maxExactAnswer);
        int binSize = 1000;

        
        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }
        for (Query q: rangeQueries) {
            Main.h.writeRangeQuery(q, dataset.size());
        }
    }
    Random rn = new Random(0);
    private void rangeSubQueries(Query q, List<Integer> attrsIdx){
       if (q.predAttrs.size() == 2) {
                return;
        }
        int numPredicates = q.predAttrs.size();
        for (int i = 0; i < numPredicates; i++) {
            List<Integer> subQueryAttrs = new ArrayList<>(attrsIdx);
            subQueryAttrs.remove(q.predAttrs.get(i)); // Remove one predicate at a time
            boolean validQuery = true;
            // 3) make query with those attributes
            StringBuilder queryString = new StringBuilder();
            for (int attr = 0; attr < Main.numAttributes; attr++) {
                if (subQueryAttrs.contains(attr)) {
                    if (!attrsToRead[attr]) {
                        continue;
                    }
                    AttributeStats a = dStats[attr];
                    if (a.dontIncludeAttribute(new StringBuilder(queryString))) {
                        validQuery = false;
                        break;
                    }
                    long value1 = rn.nextLong((minMaxValues[attr][1] - minMaxValues[attr][0]) + 1) + minMaxValues[attr][0];
                    long value2 = rn.nextLong((minMaxValues[attr][1] - minMaxValues[attr][0]) + 1) + minMaxValues[attr][0];
                    if (value1 == -999 || value2 == -999) {
                        validQuery = false;
                        break;
                    }
                    long low = Math.min(value1, value2);
                    long high = Math.max(value1, value2);
                    if (queryString.toString().equals("")) {
                        queryString.append(a.attributeName).append(" in ").append(low).append(",").append(high);
                    } else {
                        queryString.append(" and ").append(a.attributeName).append(" in ").append(low).append(",").append(high);
                    }
                }
            }
            if (!validQuery) {
                continue;
            }
            Query subQ = new Parser(queryId).parse(queryString.toString());
            queryId++;
            for (Query q2 : altRangeQueries) {
                if (q2.rangeQueryEqual(subQ)) {
                    validQuery = false;
                    break;
                }
            }
            if (!validQuery) {
                continue;
            }
            if (subQ.predAttrs.size() == 1) {
                continue;
            }
            // 4) add to altQueries with count 0
            subQ.exactAnswer = 0;
            altRangeQueries.add(subQ);
            genSubQueries++;

            if (rn.nextInt(200) == 0) {
                rangeSubQueries(subQ, subQueryAttrs);
            }

            for (int attr : subQ.predAttrs) {
                attributesInWorkload[attr] = true;
            }
            queryWithPredicates[subQ.predAttrs.size() - 1] = true;
        }

    }

    void batchExactRange(long[] rec, ArrayList<Query> workload) {
        for (Query q2 : workload) {
            // check if record all higher than q.lower and all lower than q.higher
            boolean valid = true;
            for (int i = 0; i < q2.predAttrs.size(); i++) {
                int attr = q2.predAttrs.get(i);
                if (rec[attr] < q2.lower[attr] || rec[attr] > q2.upper[attr]) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                q2.exactAnswer++;
            }
        }

    }

    public static List<Integer> getRandomKeys(LinkedHashMap<Integer, ArrayList<Integer>> linkedHashMap, int seed, int count) {
        if (linkedHashMap.isEmpty()) {
            throw new NullPointerException("The map must not be empty");
        }

        if (count <= 0) {
            throw new IllegalArgumentException("Count must be greater than zero");
        }

        // Convert the key set to an array
        Object[] keyArray = linkedHashMap.keySet().toArray();

        // Initialize a list to store the random keys
        List<Integer> randomKeys = new ArrayList<>();

        if (count > keyArray.length) {
            for (Object o : keyArray) {
                randomKeys.add((Integer) o);
            }
        } else {
            // Generate X random keys
            Random random = new Random(seed);
            for (int i = 0; i < count; i++) {
                int randomIndex = random.nextInt(keyArray.length);
                randomKeys.add((Integer) keyArray[randomIndex]);
            }
        }
        return randomKeys;
    }

    public String[] synthInfoToWrite(int id, long[] values) {
        String[] data = new String[Main.numAttributes + 1];
        data[0] = Integer.toString(dataset.size());
        data[1] = Integer.toString(Main.numAttributes);
        data[2] = Integer.toString(numZipfAttrs);
        data[3] = Double.toString(zipfAlpha);
        data[4] = Integer.toString(id);
        data[5] = Arrays.toString(values);
        return data;
    }

    public void synthDataset(int numRecords, int numZipfianAttributes, double zipfAlpha, boolean differAlphas) throws IOException {
        long[][] zipfData = ZipfGenerator.zipfData(numRecords, numZipfianAttributes, 10000, zipfAlpha);
        for (int i = 0; i < numRecords; i++) {
            long[] values = zipfData[i];
            this.dataset.add(new RecordSynth(i, values));
            String[] data = synthInfoToWrite(i, values);
            h.initSynthDataset(data, numRecords, numZipfianAttributes, zipfAlpha, differAlphas);
            h.writeSynthDataset(data, numRecords, numZipfianAttributes, zipfAlpha, differAlphas);

        }
        System.out.println("Dataset size Synthetic set: " + dataset.size());
    }

    public void synthWorkload() throws IOException {
        h.initWorkloadFileWriter(dataset.size());
        Random rn = new Random();
        for (int i = 0; i < dataset.size(); i++) {
            if (i % 1000000 == 0) {
                System.out.println("Generating pot queries: " + i);
            }
            if (rn.nextInt(10000) != 0) {
                continue;
            }
            boolean validQuery = true;
            StringBuilder queryString = new StringBuilder();
            for (int attr = 0; attr < Main.numAttributes; attr++) {
                String attrName = Parser.getAttributeName(attr);
                long value = dataset.get(i).getRecord()[attr];
                if (queryString.toString().equals("")) {
                    queryString.append(attrName).append(" = ").append(value);
                } else {
                    queryString.append(" and ").append(attrName).append(" = ").append(value);
                }
            }
            Query q = new Parser(i).parse(queryString.toString());
            for (Query q2 : altQueries) {
                if (q2.pointQueryEqual(q)) {
                    validQuery = false;
                    break;
                }
            }
            if (!validQuery) {
                continue;
            }
            if (q.predAttrs.size() == 1) {
                continue;
            }
            // 4) add to altQueries with count 0
            q.exactAnswer = 0;
            altQueries.add(q);
            ArrayList<Integer> predAttrs = new ArrayList<>(Arrays.asList(0,1,2,3,4));
            addSubQueriesSynth(q, dataset.get(i).getRecord(), predAttrs);
            for (int attr : q.predAttrs) {
                attributesInWorkload[attr] = true;
            }
            queryWithPredicates[q.predAttrs.size() - 1] = true;
        }
        Main.logger.info("Created " + altQueries.size() + " queries");
        System.out.println("Number of queries to check: " + altQueries.size());

        // After for loop, new for loop over all records in dataset
        double maxExactAnswer = 0;
        long startTime1 = System.currentTimeMillis();
        boolean satisfies;
        long[] rec;
        for (int i = 0; i < dataset.size(); i++) {
            if (i % 10000 == 0) {
                System.out.println("Processing record " + i);
            }

            rec = dataset.get(i).getRecord();
            batchExact(rec, altQueries);
        }
        for (Query q : altQueries) {
            if (q.exactAnswer > maxExactAnswer) {
                maxExactAnswer = q.exactAnswer;
            }
        }
        long endTime = System.currentTimeMillis();
        Main.logger.info("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altQueries.size() + " ms per query");
        System.out.println("Time to compute exact answers: " + (endTime - startTime1) + " ms" +
                " for " + dataset.size() + " records" + " avg: " +
                (endTime - startTime1) / altQueries.size() + " ms per query");
        System.out.println("Max exact answer: " + maxExactAnswer);
        int binSize = 1000;
        // Step 1: Calculate the quartiles
        List<Double> exactAnswers = new ArrayList<>();
        for (Query q : altQueries) {
            exactAnswers.add(q.exactAnswer);
        }

        Collections.sort(exactAnswers);
        int n = exactAnswers.size();
        double Q1 = exactAnswers.get(n / 4);
        double Q2 = exactAnswers.get(n / 2);
        double Q3 = exactAnswers.get((3 * n) / 4);
        System.out.println("Q1: " + Q1);
        System.out.println("Q2: " + Q2);
        System.out.println("Q3: " + Q3);
        System.out.println("Max exact answer: " + maxExactAnswer);

// Step 2: Assign queries to their corresponding quartile bin
        //List<Query> queries = new ArrayList<>();
        int[] queryBins = new int[4];
        int droppedQueriesQ1 = 0;
        int droppedQueriesQ2 = 0;
        int droppedQueriesQ3 = 0;
        int droppedQueriesQ4 = 0;
        for (Query q : altQueries) {
            if (q.exactAnswer < Q1) {
                q.queryBin = 0;
                queryBins[0]++;
                queries.add(q);
            } else if (q.exactAnswer >= Q1 && q.exactAnswer < Q2) {
                q.queryBin = 1;
                queryBins[1]++;

                queries.add(q);
            } else if (q.exactAnswer >= Q2 && q.exactAnswer < Q3) {
                q.queryBin = 2;
                queries.add(q);
            } else {
                q.queryBin = 3;
                queries.add(q);
                queryBins[3]++;
            }
        }
        System.out.println("Dropped queries Q1: " + droppedQueriesQ1);
        System.out.println("Dropped queries Q2: " + droppedQueriesQ2);
        System.out.println("Dropped queries Q3: " + droppedQueriesQ3);
        System.out.println("Dropped queries Q4: " + droppedQueriesQ4);
        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }
        for (Query q: queries) {
            Main.h.writeQuery(q, dataset.size());
        }
    }

    public void addSubQueriesSynth(Query q, long[] record, List<Integer> attrs) {
        if (q.predAttrs.size() == 2) {
            return;
        }
        int numPredicates = q.predAttrs.size();
        for (int i = 0; i < numPredicates; i++) {
            List<Integer> subQueryAttrs = new ArrayList<>(attrs);
            subQueryAttrs.remove(q.predAttrs.get(i)); // Remove one predicate at a time
            boolean validQuery = true;
            // 3) make query with those attributes
            StringBuilder queryString = new StringBuilder();
            for (int attr = 0; attr < Main.numAttributes; attr++) {
                if (subQueryAttrs.contains(attr)) {
                    String attrName = Parser.getAttributeName(attr);
                    long value = record[attr];
                    if (value == -999) {
                        validQuery = false;
                        break;
                    }
                    if (queryString.toString().equals("")) {
                        queryString.append(attrName).append(" = ").append(value);
                    } else {
                        queryString.append(" and ").append(attrName).append(" = ").append(value);
                    }
                }
            }
            if (!validQuery) {
                continue;
            }
            Query subQ = new Parser(queryId).parse(queryString.toString());
            queryId++;
            for (Query q2 : altQueries) {
                if (q2.pointQueryEqual(subQ)) {
                    validQuery = false;
                    break;
                }
            }
            if (!validQuery) {
                continue;
            }
            if (subQ.predAttrs.size() == 1) {
                continue;
            }
            // 4) add to altQueries with count 0
            subQ.exactAnswer = 0;
            altQueries.add(subQ);
            addSubQueriesSynth(subQ, record, subQueryAttrs);
            for (int attr : subQ.predAttrs) {
                attributesInWorkload[attr] = true;
            }
            queryWithPredicates[subQ.predAttrs.size() - 1] = true;
        }
    }



    /////////// RANGE queries ///////////////////////////////////////////////////////////////////////////////////////
    ArrayList<Query> altRangeQueries = new ArrayList<>();
    ArrayList<Query> rangeQueries = new ArrayList<>();

    public void rangeWorkload() throws IOException {


        // Make a first range query for debugging:
        StringBuilder queryString = new StringBuilder();
        for (int attr = 0; attr < Main.numAttributes; attr++) {
            String attrName = Parser.getAttributeName(attr);
            long value1 = dataset.get(0).getRecord()[attr];
            long value2 = dataset.get(1).getRecord()[attr];
            long low = Math.min(value1, value2);
            long high = Math.max(value1, value2);
            if (queryString.toString().equals("")) {
                queryString.append(attrName).append(" in ").append(low).append(",").append(high);
            } else {
                queryString.append(" and ").append(attrName).append(" in ").append(low).append(",").append(high);
            }
        }
        Query q = new Parser(0).parse(queryString.toString());
        q.exactAnswer = 0;
        altRangeQueries.add(q);


        // Exact answer
        for (int i = 0; i < dataset.size(); i++) {
            if (i % 1000000 == 0) {
                System.out.println("Generating exact answers: " + i);
            }
            long[] record = dataset.get(i).getRecord();
            for (Query q2 : altRangeQueries) {
                // check if record all higher than q.lower and all lower than q.higher
                boolean valid = true;
                for (int attr = 0; attr < Main.numAttributes; attr++) {
                    if (record[attr] < q2.lower[attr] || record[attr] > q2.upper[attr]) {
                        valid = false;
                        break;
                    }
                }
                if (valid) {
                    q2.exactAnswer++;
                }
            }
        }

        if (q.exactAnswer > 0) {
            rangeQueries.add(q);
        }

        for (int attr : q.predAttrs) {
            attributesInWorkload[attr] = true;
        }
        queryWithPredicates[q.predAttrs.size() - 1] = true;
        for (int i = 0; i <= Main.numAttributes - 1; i++) {
            if (queryWithPredicates[i]) {
                this.distinctPredSize++;
            }
        }
    }


    /////////////// Exact Solution ///////////////////////////////////////////////////////////////////////////////////////
    int exactSolution(Query q) {
        int count = 0;
        for (Record r : dataset) {
            for (int i = 0; i < q.predAttrs.size(); i++) {
                if (q.rangeAttrs.get(i)) {
                    if (r.getRecord()[q.predAttrs.get(i)] < q.getLower()[q.predAttrs.get(i)] || r.getRecord()[q.predAttrs.get(i)] > q.getUpper()[q.predAttrs.get(i)])
                        break;
                } else {
                    if (!Objects.equals(r.getRecord()[q.predAttrs.get(i)], q.getRecord()[q.predAttrs.get(i)])) {
                        break;
                    }
                }
                if (i == q.predAttrs.size() - 1) { // Increase count if all predicates are satisfied
                    count++;
                }
            }
        }
        return count;
    }

    public String getMemoryUsage() {
        int usedAttrs = 0;
        for (int i = 0; i < Main.numAttributes; i++) {
                if (attributesInWorkload[i]) {
                    usedAttrs++;
                }
        }
        long usg = (long) dataset.size() * (usedAttrs * 32L + 32); // unit is bits
        return Long.toString(usg);
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(dataset.size());
        out.writeInt(queries.size());
        out.writeObject(setting);
        for (Record r: dataset) {
            out.writeObject(r);
        }
        for (Record r: queries) {
            out.writeObject(r);
        }
        out.writeObject(queryWithPredicates);
        out.writeInt(queryId);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        int numQueries = in.readInt();
        setting = (String) in.readObject();
        for (int i = 0; i < size; i++) {
            dataset.add((Record) in.readObject());
        }
        for (int i = 0; i < numQueries; i++) {
            queries.add((Query) in.readObject());
        }
        queryWithPredicates = (boolean[]) in.readObject();
        queryId = in.readInt();
    }

    public int getCardinality(int j) {
        Set<Long> distinctValues = dataset.stream().map(r -> r.getRecord()[j]).collect(Collectors.toSet());
        long cardinality = distinctValues.size();
        System.out.println("Attribute "+ j + " with name: "+ Parser.getAttributeName(j) + " cardinality: " + cardinality);
        // Get max value for attribute j
        long maxValue = dataset.stream().map(r -> r.getRecord()[j]).max(Long::compare).get();
        System.out.println("Attribute "+ j + " with name: "+ Parser.getAttributeName(j) + " max value: " + maxValue);
        // Get needed bits to store attribute j
        int maxbits = (int) Math.ceil(Math.log(maxValue) / Math.log(2));
        System.out.println("Attribute "+ j + " with name: "+ Parser.getAttributeName(j) + " bits: " + maxbits);

        // Do same for minimum
        long minValue = dataset.stream().map(r -> r.getRecord()[j]).min(Long::compare).get();
        System.out.println("Attribute "+ j + " with name: "+ Parser.getAttributeName(j) + " min value: " + minValue);
        // Get needed bits to store attribute j
        int minbits = (int) Math.ceil(Math.log(minValue*-1) / Math.log(2));
        System.out.println("Attribute "+ j + " with name: "+ Parser.getAttributeName(j) + " bits for minimum: " + minbits);
        return maxbits;
    }
}
