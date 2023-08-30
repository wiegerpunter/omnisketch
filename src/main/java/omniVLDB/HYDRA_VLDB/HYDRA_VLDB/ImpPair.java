/* (C)2021 */
package omniVLDB.HYDRA_VLDB.HYDRA_VLDB;

import java.io.Serializable;

public class ImpPair implements Serializable {
    public String key;
    public int value;

    public ImpPair(String aKey, int aValue) {
        key = aKey;
        value = aValue;
    }
}

class StringPair {
    public String key;
    public String value;

    public StringPair(String aKey, String aValue) {
        key = aKey;
        value = aValue;
    }
}
