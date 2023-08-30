/* (C)2021 */
package omniVLDB.HYDRA_VLDB.HYDRA_VLDB;

import omniVLDB.Record;
import omniVLDB.*;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;

/** Structure for storing the statistics in a Hydra Sketch. */
// implements KryoSerializable
public class ImpHydraStruct extends Synopsis implements Serializable {

    //public ImpUnivMonStruct[][] hydraSketch;
    public ImpCountSketchStruct[][] hydraSketch;
    public int totalAdded = 0;
    //public ImpUnivMonStruct bigUM;
    public float epsilon;
    //public float gamma;
    public int w; // col
    public int d; // row
    public int numRows;
    public int countersPerRow;
    public int levels;
    //public final int bigum_levels = 22;
    public int k;
    //public int data_dim;
    //public int[][] filters;
    public static XXHash64 hashFunc = XXHashFactory.fastestInstance().hash64();
    public float total_memory;
    public int hs_seed;
    public int um_seed; // for CS in UM
    public int estimate_index;
    public int total_HH_size;
    //public float[] query_bias;
    //public int useless_count; // for speedup test
    //public static final Boolean Hydra_Merge_Sketch = false;

    //public static ExecutorService executor;

    //public String debug_on;
    //public String prefix;
    //public Boolean spark_init;
    SubPopMap subPopMap;

    /*public ImpHydraStruct(HashMap<String, Integer> configMap, String debug_on, String prefix) {
        numRows = configMap.get("numRows"); // for CS in univmon
        countersPerRow = configMap.get("countersPerRow"); // for CS in univmon
        levels = configMap.get("levels");
        k = configMap.get("k"); // top k
        d = configMap.get("d");
        w = configMap.get("w");
        this.debug_on = debug_on;
        this.prefix = prefix;

        debug();

        /*spark_init = (configMap.get("spark_init") == 1);
        if (spark_init) return;
        */
        /*init_hydra_struct();
    }*/
    int memoryUsage = 0;
    boolean[] attributesInWorkload;
    ArrayList<Integer> attrsIdx;
    int numAttributes = 0;
    //ArrayList<boolean[]> validSubpop;// = new ArrayList<boolean[]>();
    ArrayList<ArrayList<Integer>> validSubpop;
    public ImpHydraStruct(long ram, int depthRoot, int widthRoot, int depthCS, int widthCS, ArrayList<Integer> attrsIdx) {
        this.ram = ram;
        this.setting = "Hydra";
        this.attrsIdx = attrsIdx;
        numAttributes = attrsIdx.size();
        d = depthRoot;
        w = widthRoot;
        numRows = depthCS;
        countersPerRow = widthCS;
        levels = 15;
        k = 30;
        parameters = new int[] {d, w, numRows, countersPerRow, levels, k};
        memoryUsage = d * w * numRows * countersPerRow * 32;
        init_hydra_struct();
        validSubpop = new ArrayList<ArrayList<Integer>>();
        //validSubpop = subPopMap.getValidSubPopulations();

    }

    public void init_hydra_struct() {
        subPopMap = new SubPopMap(attrsIdx);
        estimate_index = d / 2; // median

        total_HH_size = 0;

        Random hsGenerator = utils.setHSSeed();
        this.hs_seed = utils.getSeedRandom(hsGenerator);

        // init UM seed
        Random umGenerator = utils.setUMSeed();
        this.um_seed = utils.getSeedRandom(umGenerator);


        //hydraSketch = new ImpUnivMonStruct[d][w];
        hydraSketch = new ImpCountSketchStruct[d][w];

        for (int d_indx = 0; d_indx < d; d_indx++) {
            for (int w_indx = 0; w_indx < w; w_indx++) {
                hydraSketch[d_indx][w_indx] = new ImpCountSketchStruct(k, numRows, countersPerRow);
                        //new ImpUnivMonStruct(levels, numRows, countersPerRow, k, this.um_seed);
            }
        }
    }

    /*public void updateBatch(ArrayList<StringPair> datas) {
        if (executor == null) executor = Executors.newFixedThreadPool(d);
        if (spark_init) {
            spark_init = false;
            init_hydra_struct();
        }

        ArrayList<UpdateTask> taskList = new ArrayList<UpdateTask>();
        for (int i = 0; i < d; i++) {
            UpdateTask task = new UpdateTask(hydraSketch[i], hs_seed, i, w, numRows, countersPerRow, levels, datas);
            taskList.add(task);
        }
        try {
            List<Future<Integer>> resultList = executor.invokeAll(taskList);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    */
    //public void updateOne(String key, String value) {
    public void updateOne(String sp, String key, String value) {
        /*if (spark_init) {
            spark_init = false;
            init_hydra_struct();
        }*/

        // calculate CS position
        String insert = sp + "-" + key + ":" + value;
        //String insert = key + ":" + value;
        byte[] byte_kv = utils.StringToByte(insert);
        long hash_kv_long = hashFunc.hash(byte_kv, 0, byte_kv.length, um_seed);
        int[] hash_kv = utils.getHashArray(hash_kv_long);
        int[] pos = new int[numRows];
        int[] sign = new int[numRows];
        for (int r = 0; r < numRows; r++) {
            pos[r] = hash_kv[r] % countersPerRow;
            sign[r] = 2 * ((hash_kv[r] >> 15) & 1) - 1;
        }
        // calculate bottom layer
        // current only support level <= 17
        //int bottom_layer_num = ImpUnivMonStruct.findBottomLayerNum(hash_kv[3], levels);

        //byte[] byte_key = utils.StringToByte(key);
        byte[] byte_key = utils.StringToByte(sp);
        long hash_key_long = hashFunc.hash(byte_key, 0, byte_key.length, hs_seed);
        int[] hash_key = utils.getHashArray(hash_key_long);

        for (int d_indx = 0; d_indx < d; d_indx++) {
            int hash = hash_key[d_indx] % w;
            //ImpUnivMonStruct umSketch = hydraSketch[d_indx][hash];
            ImpCountSketchStruct umSketch = hydraSketch[d_indx][hash];
            //umSketch.update(insert, pos, sign, bottom_layer_num);
            umSketch.add(pos, sign);
            /*{ // to test speed up of one big hash
                // final Boolean Many_Hash = true;
                final boolean Many_Hash = false;
                if (Many_Hash) {
                    // need to hash 9 times in total instead of 2
                    for (int rep = 1; rep <= 2; rep++) {
                        useless_count +=
                                (int)
                                        hashFunc.hash(
                                                byte_kv,
                                                0,
                                                byte_kv.length,
                                                1L << (100 * d_indx + rep));
                    }
                }
            }*/
        }
    }

    //public float[] queryOneNoCheck(String key) {
    public float[] queryOneNoCheck(String sp, String key) {
        float[] l1_list = new float[d];
        //float[] entropy_list = new float[d];
        //float[] l2_list = new float[d];
        //float[] card_list = new float[d];
        //byte[] byte_key = utils.StringToByte(key);
        byte[] byte_key = utils.StringToByte(sp);
        long hash_key_long = hashFunc.hash(byte_key, 0, byte_key.length, hs_seed);
        int[] hash_key = utils.getHashArray(hash_key_long);

        for (int i = 0; i < d; i++) {
            int hash = hash_key[i] % w;
            ImpCountSketchStruct umSketch = hydraSketch[i][hash];
            //ImpUnivMonStruct umSketch = hydraSketch[i][hash];
            /*if (!onlyL1) {
                float tmp_entropy = umSketch.calcEntropy(key);
                float tmp_l2 = umSketch.calcL2(key);
                float tmp_card = umSketch.calcCard(key);
                float tmp_l1 = umSketch.calcL1(key);
                // System.out.printf("dbg %d\n", umSketch.
                l1_list[i] = tmp_l1;
                //entropy_list[i] = tmp_entropy;
                //l2_list[i] = tmp_l2;
                //card_list[i] = tmp_card;
            }*/
            byte[] byte_kv = utils.StringToByte(sp + "-" + key + ":1");
            long hash_kv_long = hashFunc.hash(byte_kv, 0, byte_kv.length, um_seed);
            int[] hash_kv = utils.getHashArray(hash_kv_long);
            int[] pos = new int[numRows];
            int[] sign = new int[numRows];
            for (int r = 0; r < numRows; r++) {
                pos[r] = hash_kv[r] % countersPerRow;
                sign[r] = 2 * ((hash_kv[r] >> 15) & 1) - 1;
            }
            float tmp_l1 = umSketch.inspectValue(pos, sign);
            /*if (tmp_l1 > 0) {
                System.out.println("Saw a positive L1");
            } else {
                System.out.println("Saw a negative L1");
            }*/
            // System.out.printf("dbg %d\n", umSketch.
            l1_list[i] = tmp_l1;
        }
        /*if (!onlyL1) {
            float estimate_entropy = utils.MinOfThree(entropy_list);
            float estimate_l2 = utils.MinOfThree(l2_list);
            float estimate_card = utils.MinOfThree(card_list);
            float estimate_l1 = utils.MinOfThree(l1_list);
            return new float[]{estimate_entropy, estimate_l2, estimate_l1, estimate_card};
        } else {
            */
        float estimate_l1 = utils.MinOfThree(l1_list);
        return new float[]{estimate_l1};
        //}
    }

    /*public ArrayList<QueryResult> queryBatchNoCheck(HashSet<String> hash_keys) {
        ArrayList<String> keys = new ArrayList<>(hash_keys);
        return queryBatchNoCheck(keys);
    }*/

    /*public ArrayList<QueryResult> queryBatchNoCheck(ArrayList<String> keys) {
        if (executor == null) executor = Executors.newFixedThreadPool(d);

        ArrayList<QueryTask> taskList = new ArrayList<QueryTask>();
        int keys_len = keys.size();
        int[][] hashs = new int[d][keys_len]; // which UM for key in keys
        for (int i = 0; i < keys_len; i++) {
            String key = keys.get(i);
            byte[] byte_key = utils.StringToByte(key);
            long hash_key_long = hashFunc.hash(byte_key, 0, byte_key.length, hs_seed);
            int[] hash_key = utils.getHashArray(hash_key_long);
            for (int j = 0; j < d; j++) {
                hashs[j][i] = hash_key[j] % w;
            }
        }
        for (int i = 0; i < d; i++) {
            QueryTask task = new QueryTask(hydraSketch[i], keys, hashs[i]);
            taskList.add(task);
        }
        List<Future<ArrayList<QueryResult>>> resultList;
        ArrayList<ArrayList<QueryResult>> query_result = new ArrayList<ArrayList<QueryResult>>();
        try {
            resultList = executor.invokeAll(taskList);
            for (Future<ArrayList<QueryResult>> r : resultList) {
                query_result.add(r.get());
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }

        ArrayList<QueryResult> query_errors = new ArrayList<QueryResult>();
        float[] entropy_list = new float[d];
        float[] l2_list = new float[d];
        float[] l1_list = new float[d];
        float[] card_list = new float[d];
        for (int i = 0; i < keys.size(); i++) {
            for (int j = 0; j < d; j++) {
                QueryResult qr = query_result.get(j).get(i);
                entropy_list[j] = qr.entropy;
                l2_list[j] = qr.l2;
                l1_list[j] = qr.l1;
                card_list[j] = qr.card;
            }

            float estimate_entropy = utils.MedianOfThree(entropy_list);
            float estimate_l2 = utils.MedianOfThree(l2_list);
            float estimate_l1 = utils.MedianOfThree(l1_list);
            float estimate_card = utils.MedianOfThree(card_list);

            QueryResult qr =
                    new QueryResult(estimate_entropy, estimate_l2, estimate_l1, estimate_card);
            qr.key = keys.get(i);
            query_errors.add(qr);
        }
        return query_errors;
    }*/

    /*public void merge(ImpHydraStruct other) {
        if (other.hydraSketch == null) return;

        this.total_HH_size = 0;
        for (int d_indx = 0; d_indx < this.d; d_indx++) {
            for (int w_indx = 0; w_indx < this.w; w_indx++) {
                this.hydraSketch[d_indx][w_indx].mergeNaive(other.hydraSketch[d_indx][w_indx]);
                for (int l_indx = 0; l_indx < this.levels; l_indx++) {
                    total_HH_size += this.hydraSketch[d_indx][w_indx].um[l_indx].topK.size;
                }
            }
        }

        this.useless_count += other.useless_count;
    }*/

    /*private void readUM(ObjectInputStream in, ImpUnivMonStruct um)
            throws ClassNotFoundException, IOException {
        um.totalCount = in.readInt();
        for (int l_indx = 0; l_indx < um.layer; l_indx++) {
            // read counters
            if (Hydra_Merge_Sketch) {
                int counter_allocate = in.readInt();
                if (counter_allocate == 1) {
                    um.um[l_indx].allocate_cs();
                    for (int i = 0; i < this.numRows * this.countersPerRow; i++) {
                        um.um[l_indx].cs[i] = in.readInt();
                    }
                }
            }

            // read heap
            int size = in.readInt();
            MinHeap topK = um.um[l_indx].topK;
            topK.allocateHeap(size);
            for (int k_indx = 0; k_indx < size; k_indx++) {
                String key = in.readUTF();
                int value = in.readInt();
                // topK.update(key, value, value);
                topK.readback(key, value);
            }
        }
    }*/

    /*private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        w = in.readInt();
        d = in.readInt();
        numRows = in.readInt();
        countersPerRow = in.readInt();
        levels = in.readInt();
        k = in.readInt();
        /*spark_init = in.readBoolean();
        if (spark_init) return;
        *//*
        init_hydra_struct();

        for (int d_indx = 0; d_indx < this.d; d_indx++)
            for (int w_indx = 0; w_indx < this.w; w_indx++) {
                ImpUnivMonStruct um = this.hydraSketch[d_indx][w_indx];
                readUM(in, um);
            }
        total_HH_size = in.readInt();
        useless_count = in.readInt();
    }*/

    /*private void writeUM(ObjectOutputStream out, ImpUnivMonStruct um) throws IOException {
        out.writeInt(um.totalCount);
        for (int l_indx = 0; l_indx < um.layer; l_indx++) {
            // write out counters
            if (Hydra_Merge_Sketch) {
                if (um.um[l_indx].cs == null) out.writeInt(0);
                else {
                    out.writeInt(1);
                    for (int i = 0; i < this.numRows * this.countersPerRow; i++) {
                        out.writeInt(um.um[l_indx].cs[i]);
                    }
                }
            }

            // write out heap
            MinHeap topK = um.um[l_indx].topK;
            out.writeInt(topK.size);
            for (int i = 0; i < topK.size; i++) {
                ImpPair hh = topK.Heap[i];
                out.writeUTF(hh.key);
                out.writeInt(hh.value);
            }
            total_HH_size += topK.size;
        }
    }*/

    /*private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(w);
        out.writeInt(d);
        out.writeInt(numRows);
        out.writeInt(countersPerRow);
        out.writeInt(levels);
        out.writeInt(k);

        /*out.writeBoolean(spark_init);
        if (spark_init) return;
        *//*
        this.total_HH_size = 0;
        for (int d_indx = 0; d_indx < this.d; d_indx++)
            for (int w_indx = 0; w_indx < this.w; w_indx++) {
                ImpUnivMonStruct um = this.hydraSketch[d_indx][w_indx];
                writeUM(out, um);
            }
        out.writeInt(total_HH_size);
        out.writeInt(useless_count);
    }*/

    /*public void printTopHeap() {
        for (int d_indx = 0; d_indx < this.d; d_indx++) {
            for (int w_indx = 0; w_indx < this.w; w_indx++) {
                System.out.printf("top heap, idx %d %d\n", d_indx, w_indx);
                hydraSketch[d_indx][w_indx].printTopHeap();
            }
        }
    }*/

    /*private float calcRelativeError(float real, float estimate) {
        if (Math.abs(estimate) < 1e-6) return Math.abs(real);
        float err = Math.abs(real - estimate) / real * 100;
        if (Float.isInfinite(err)) return 100.0f;
        if (Float.isNaN(err)) return 100.0f;
        return err;
    }*/

    /*private float[] calcRelativeError(float real, float[] estimate_list) {
        float[] ret = new float[d];
        for (int i = 0; i < d; i++) {
            ret[i] = calcRelativeError(real, estimate_list[i]);
        }
        return ret;
    }*/

    /*private void debug() {
        System.out.println("d: " + d);
        System.out.println("w: " + w);
        System.out.println("numRows: " + numRows);
        System.out.println("countersPerRow: " + countersPerRow);
        System.out.println("k: " + k);
        System.out.println("levels: " + levels);

        int csMemory = 4 * numRows * countersPerRow;
        int heapMemory = 12 * k;
        final float LOAD_FACTOR = 0.75f;
        int heap_hashtable_Memory = (int) (heapMemory / LOAD_FACTOR);
        float um_memory = (csMemory + heapMemory + heap_hashtable_Memory) * levels / 1000;
        this.total_memory = w * d * um_memory;

        System.out.printf("UM memory(KB): %.2f\n", um_memory);
        System.out.printf("total_memory(KB): %.2f\n", total_memory);
    }*/

    @Override
    public void add(Record record) {

        long[] rec = record.getRecord();
        Set<Integer> keySet = subPopMap.subPopulations2.keySet();
        for (int sp : keySet) {
            ArrayList<Integer> subpop = subPopMap.subPopulations2.get(sp);
            StringBuilder key = new StringBuilder();
            for (int i: attrsIdx) {
                if (subpop.contains(i))
                    key.append(rec[i]);
            }
            //String insert = sp + "-" + key;
            totalAdded++;
            String keyString = String.valueOf(key);
            updateOne(Integer.toString(sp), keyString, "1");
            //updateOne(insert, "1");
        }

    }

    @Override
    public int query(Query query) {
        ArrayList<Integer> attrsIdx = new ArrayList<Integer>();
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < Main.numAttributes; i++) {
            if (query.pointAttrs[i]) {
                attrsIdx.add(i);
                value.append(query.getRecord()[i]);
            }
        }
        int subPop = subPopMap.getSubPop(attrsIdx);
        String insert = subPop + "-" + value;
        //float[] ret = queryOneNoCheck(insert);
        String key = String.valueOf(value);
        float[] ret = queryOneNoCheck(Integer.toString(subPop), key);
        return (int) ret[0];
    }

    public int rangeQuery(Query q) {
        return 0;
    }

    @Override
    public void reset() {
        init_hydra_struct();
    }

    @Override
    public long getMemoryUsage() {
       return memoryUsage;
    }
}
