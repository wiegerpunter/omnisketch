package omniVLDB;

import com.opencsv.exceptions.CsvValidationException;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapNativeException;

import java.io.IOException;
import java.util.Objects;
import java.util.logging.*;

public class Main {
    public static int repetition;

    public static double eps = 0.1;
    public static double delta = 0.1;
    public static double epsCM;
    public static double epsDS = eps;
    public static double deltaCM;
    public static double deltaDS;
    public static double qTarget = 0.01;
    public static int seed = 3;
    public static int numAttributes;
    public static int depth;// = (int) Math.ceil(Math.log(1/deltaCM)/Math.log(Math.exp(1))); // Depth of sketch (number of hash functions)
    public static int width;// = (int) Math.ceil(Math.exp(1)/epsCM); // Width of sketch (number of buckets)
    public static int streamSize;
    public static int maxLevel = 25;// =Math.log(1 + (streamsize/width)/(numPred* Math.log(1/deltaB)))/Math.log(2);
    public static double epsB;// = Math.sqrt((numPred * Math.log(1/deltaB) * (Math.pow(2, maxLevel + 1) - 1))/((double) streamSize/width));
    public static int maxSize;// = (int) Math.ceil(Math.log(1/deltaDS)/Math.log(Math.exp(1))/(qTarget * Math.pow(epsDS, 2))* (1 + epsB));
    public static int b;
    public static String datasetName;
    public static String datasetFolder;
    public static int numQueries;

    public static String workloadFilename;
    public static String fileStartCondition = "03110";
    public static int warmupNumber;
    public static int dyadicRangeBits = 33;
    static int numFiles = 1;

    static String setting;
    static boolean readAllFiles = true; // Set true if all files should be read, false if only the first file should be read
    public static boolean rangeQueries = false;
    static boolean createNewWorkload = false; // Set true if new workload should be created
    static boolean createNewRangeWorkload = false;
    public static boolean useMultNumAttributes = false; // Set true if want to test for multiple number of attributes instead of just all attributes.
    static boolean useTighterBound = false; // Set true if want to check tighter bound in sensitivity analysis
    public static boolean useDS = false; // True if DS is used, false if Kminwise hashing is used. Should be false by default.
    public static boolean sanityBound = false; // True if parameter setting is decided by sanity bound. False if decided by how much memory is left for B.
    public static boolean runOnODC = true; // True if running on ODC, false if running on local machine.
    public static String outputFolder;
    public static String readFolder;
    public static Logger logger = Logger.getLogger("logger");
    // Set logger level to info
    static {logger.setLevel(Level.INFO);}
    public static FileHandler fileHandler;
    public static int filesToRead = 1;
    public static Helper h;
    public static boolean useWarmup = true;
    public static boolean writeSNMP = false;
    public static boolean sensitivityAnalysis = false;
    public static int sensitivityNumberOfRecords = 5000000;


    public static void main(String[] args) throws IOException, NotOpenException, PcapNativeException, CsvValidationException {
        setting = args[0];
        Main.datasetName = args[1];
        Main.repetition = Integer.parseInt(args[2]);

        if (Main.datasetName.equals("CAIDA")) {
            Main.warmupNumber = 1000000;
        } else if (Main.datasetName.equals("SNMP")) {
            Main.warmupNumber = (int) 3e5;
        } else {
            Main.warmupNumber = (int) 3e5;
        }
        if (runOnODC) {
            readFolder = "/home/";
            outputFolder = "/home/exp5/output/";
            LogManager.getLogManager().readConfiguration(
                    Main.class.getResourceAsStream("/loggingODC.properties")
            );
        } else {
            readFolder = "./";
            outputFolder = "./output/";
            LogManager.getLogManager().readConfiguration(
                    Main.class.getResourceAsStream("/loggingLocal.properties")
            );
        }

        // dt is date and hour and minute
        //String date_and_time = java.time.LocalDateTime.now().toString().replace(":", "-").replace(".", "-");
        try {
            fileHandler = new FileHandler(outputFolder + "logs/myLogs1.log");
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Starting program");
        Main.workloadFilename = "workload_" + Main.datasetName + "_N" + Integer.toString(Main.filesToRead) + ".csv";
        h = new Helper();

        if (Objects.equals(setting,"Compare Baselines")) {
            CompareBaselines cb = new CompareBaselines(h);
            cb.run();
        } else if (Objects.equals(setting,"Compare Estimators")) {
            CompareEstimators ce = new CompareEstimators(h);
            ce.run();
        }  else if (Objects.equals(setting,"Compare Distributions")) {
            CompareDistributions cd = new CompareDistributions(h);
            cd.run();
        } else if (Objects.equals(setting,"Scalability")) {
            Scalability sc = new Scalability(h);
            sc.run();
        } else if (Objects.equals(setting, "Range Queries")) {
            RangeQueries rq = new RangeQueries(h);
            rq.run();
        } else {
            logger.severe("Invalid argument");
            logger.severe(setting + " & " + args[1]);
        }
    }

    static Synopsis s;

}


