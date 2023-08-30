/* (C)2021 */
package omniVLDB.HYDRA_VLDB.HYDRA_VLDB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

public class utils {
    public static final byte[] intToByteArray(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }

    public static final byte[] intToByteArrayBB(int value) {
        byte val1 = (byte) (value >>> 24);
        byte val2 = (byte) (value >>> 16);
        byte val3 = (byte) (value >>> 8);
        byte val4 = (byte) value;

        if ((val1 == val2) && (val2 == val3) && (val3 == 0)) return new byte[] {val4};
        else if ((val1 == val2) && (val2 == 0)) return new byte[] {val3, val4};
        else if (val1 == 0) return new byte[] {val2, val3, val4};
        else return new byte[] {val1, val2, val3, val4};
    }

    public static final int ByteArrayToInt(byte[] bytes) {
        if (bytes == null) return 0;
        int length = bytes.length;
        if (length == 0) return 0;
        else if (length == 1) return ((bytes[0]) << 0);
        else if (length == 2) return ((bytes[0] & 0xFF) << 8) | ((bytes[1] & 0xFF) << 0);
        else if (length == 3)
            return ((bytes[0] & 0xFF) << 16) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 0);
        else
            return ((bytes[0] & 0xFF) << 24)
                    | ((bytes[1] & 0xFF) << 16)
                    | ((bytes[2] & 0xFF) << 8)
                    | ((bytes[3] & 0xFF) << 0);
    }

    public static float log2(float n) {
        return (float) (Math.log(n) / Math.log(2));
    }

    public static int getRandom() {
        return (int) (Math.random() * 1000000 + 1);
    }

    public static Random setHSSeed() {
        Random generator = new Random(42);
        return generator;
    }

    public static Random setUMSeed() {
        Random generator = new Random(420);
        return generator;
    }

    public static Random setHSeed() {
        Random generator = new Random(25);
        return generator;
    }

    public static Random setSSeed() {
        Random generator = new Random(250);
        return generator;
    }

    public static int getSeedRandom(Random generator) {
        return (int) (generator.nextFloat() * 1000000 + 13);
    }

    public static HashMap<String, Integer> ReadConfig(String configFile) {
        HashMap<String, Integer> configMap = new HashMap<String, Integer>();
        try {
            Scanner scan = new Scanner(new FileReader(configFile));
            while (scan.hasNext()) {
                String key = scan.next();
                int value = scan.nextInt();
                configMap.put(key, value);
            }
            scan.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return configMap;
    }

    public static HashMap<String, String> ReadExp(String expFile) {
        HashMap<String, String> expMap = new HashMap<String, String>();
        try {
            Scanner scan = new Scanner(new FileReader(expFile));
            while (scan.hasNext()) {
                String key = scan.next();
                String value = scan.next();
                expMap.put(key, value);
            }
            scan.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return expMap;
    }

    public static void PrintExp(HashMap<String, String> expMap) {
        for (String key : expMap.keySet()) {
            String value = expMap.get(key);
            System.out.printf("exp: %s %s\n", key, value);
        }
    }

    // input is "1,3"
    // return [1,3]
    public static int[] ParseKeyDim(String key_dim_str) {
        String[] split = key_dim_str.split(",");
        int[] key_dim = new int[split.length];
        for (int i = 0; i < split.length; i++) key_dim[i] = Integer.parseInt(split[i]);
        return key_dim;
    }

    // input is "dim0,dim1,dim2,dim3,dim4", [1,3]
    // output is "dim1|dim3"
    public static String makeKey(String[] data, int[] key_dim) {
        StringBuilder str_build = new StringBuilder();
        Boolean first = true;
        for (int i : key_dim) {
            if (!first) str_build.append("|");
            first = false;
            str_build.append(data[i]);
        }
        return str_build.toString();
    }

    // this is for query, 2.5 is entropy, 100 is l2, 10 is group_size
    // input is "dim1,dim3,2.5,100,10", 2
    // output is "dim1|dim3|"
    public static String makeKey(String[] data, int key_dim_len) {
        StringBuilder str_build = new StringBuilder();
        str_build.append(data[0]);
        for (int i = 1; i < key_dim_len; i++) {
            str_build.append("|");
            str_build.append(data[i]);
        }
        return str_build.toString();
    }

    public static HashMap<String, Integer> MakeConfig(
            int w, int d, int numRows, int countersPerRow, int levels, int k, int data_dim) {
        HashMap<String, Integer> configMap = new HashMap<String, Integer>();
        configMap.put("w", w);
        configMap.put("d", d);
        configMap.put("numRows", numRows);
        configMap.put("countersPerRow", countersPerRow);
        configMap.put("levels", levels);
        configMap.put("k", k);
        configMap.put("data_dim", data_dim);
        return configMap;
    }

    public static HashSet<Integer> ReadTopKeys(String top_key_file, int top_k) {
        HashSet<Integer> top_keys = new HashSet<Integer>();
        int count = 0;
        try {
            Scanner scan = new Scanner(new FileReader(top_key_file));
            while (scan.hasNext()) {
                if (count == top_k) break;
                top_keys.add(scan.nextInt());
                count++;
            }
            scan.close();
        } catch (Exception e) {
            System.out.println(e);
        }
        return top_keys;
    }

    public static String toString(float array[]) {
        if (array == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format(Locale.ENGLISH, "%.2f", array[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    public static byte[] StringToByte(String str) {
        byte[] data = str.getBytes();
        return data;
    }

    // copy from
    // https://stackoverflow.com/questions/29267777/most-efficient-way-of-splitting-string-in-java
    // https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
    public static String[] mySplit(final String line, final char delimiter) {
        CharSequence[] temp = new CharSequence[(line.length() / 2) + 1];
        int wordCount = 0;

        int start = 0;
        boolean inQuotes = false;
        for (int current = 0; current < line.length(); current++) {
            if (line.charAt(current) == '\"') inQuotes = !inQuotes; // toggle state
            else if (line.charAt(current) == ',' && !inQuotes) {
                temp[wordCount++] = line.substring(start, current);
                start = current + 1;
            }
        }

        temp[wordCount++] = line.substring(start); // last substring

        String[] result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);

        return result;
    }

    /**
     * Execute a bash command. We can handle complex bash commands including multiple executions (;
     * | && ||), quotes, expansions ($), escapes (\), e.g.: "cd /abc/def; mv ghi 'older ghi
     * '$(whoami)"
     *
     * @param command
     * @return true if bash got started, but your command may have failed.
     */
    public static boolean executeBashCommand(String command) {
        boolean success = false;
        System.out.println("Executing BASH command:\n   " + command);
        Runtime r = Runtime.getRuntime();
        // Use bash -c so we can handle things like multi commands separated by ; and
        // things like quotes, $, |, and \. My tests show that command comes as
        // one argument to bash, so we do not need to quote it to make it one thing.
        // Also, exec may object if it does not have an executable file as the first thing,
        // so having bash here makes it happy provided bash is installed and in path.
        String[] commands = {"bash", "-c", command};
        try {
            Process p = r.exec(commands);

            p.waitFor();
            BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";

            while ((line = b.readLine()) != null) {
                System.out.println(line);
            }

            b.close();
            success = true;
        } catch (Exception e) {
            System.err.println("Failed to execute bash with command: " + command);
            e.printStackTrace();
        }
        return success;
    }

    public static int getHash0(final long hash_long) {
        return (int) (hash_long & 0xffff);
    }

    public static int getHash1(final long hash_long) {
        return (int) ((hash_long >> 16) & 0xffff);
    }

    public static int getHash2(final long hash_long) {
        return (int) ((hash_long >> 32) & 0xffff);
    }

    public static int getHash3(final long hash_long) {
        return (int) ((hash_long >> 48) & 0xffff);
    }

    public static int[] getHashArray(final long hash_long) {
        return new int[] {
            getHash0(hash_long), getHash1(hash_long), getHash2(hash_long), getHash3(hash_long)
        };
    }

    public static int getHashHack(final long hash_long) {
        // use getHash3, 13-16 bits of getHash0/1/2
        return getHash3(hash_long)
                | ((getHash0(hash_long) & 0xf000) << 4)
                | ((getHash1(hash_long) & 0xf000) << 8)
                | ((getHash2(hash_long) & 0xf000) << 12);
    }

    public static int MedianOfThree(int a, int b, int c) {
        int my_min = Math.min(a, Math.min(b, c));
        int my_max = Math.max(a, Math.max(b, c));
        return (a + b + c) - my_min - my_max;
    }

    public static int MedianOfThree(int[] arr) {
        return MedianOfThree(arr[0], arr[1], arr[2]);
    }

    public static float MedianOfThree(float a, float b, float c) {
        float my_min = Math.min(a, Math.min(b, c));
        float my_max = Math.max(a, Math.max(b, c));
        return (a + b + c) - my_min - my_max;
    }

    public static float MedianOfThree(float[] arr) {
        return MedianOfThree(arr[0], arr[1], arr[2]);
    }

    public static float MinOfThree(float[] arr) {
        return Math.min(arr[0], Math.min(arr[1], arr[2]));
    }
}

class MyTimer {
    public long start_time;
    public String name;

    public MyTimer(String _name) {
        start_time = System.nanoTime();
        name = _name;
    }

    public void Stop() {
        float elapsed_time = (System.nanoTime() - start_time) / 1_000_000_000.0f;
        System.out.printf("%s time: %.3f\n", name, elapsed_time);
    }
}
