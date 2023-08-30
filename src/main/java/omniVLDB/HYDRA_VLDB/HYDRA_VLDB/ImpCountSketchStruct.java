/* (C)2021 */
package omniVLDB.HYDRA_VLDB.HYDRA_VLDB;

import java.io.Serializable;
//import java.util.ArrayList;
import java.util.Arrays;
//import java.util.HashMap;

public class ImpCountSketchStruct implements Serializable {
    public int[] cs;
    public int k; // top K
    public int row; // row
    public int col; // col
    //public MinHeap topK;
    //public HashMap<String, ArrayList<ImpPair>> HH_table; // speedup query for heuristic method
    //public ArrayList<ImpPair> HH_list; // speedup query

    public ImpCountSketchStruct(int k, int row, int col) {
        this.k = k;
        this.row = row;
        this.col = col;
        //this.topK = new MinHeap(k);
    }

    public void allocate_cs() {
        this.cs = new int[row * col];
    }

    // only update topK
    // return if item is HH after update
    /*public boolean updateHeap(String item, int count, int add) {
        return topK.update(item, count, add);
    }*/

    // add value to counters, return median
    public void add(int[] pos, int[] sign) { // add was initially int not void
        if (this.cs == null) allocate_cs();

        //int[] counters = new int[row];
        for (int r = 0; r < row; r++) {
            cs[r * col + pos[r]] += sign[r];
            //counters[r] = sign[r] * cs[r * col + pos[r]];
        }

        //int median = utils.MedianOfThree(counters[0], counters[1], counters[2]);
        //if (median <= 0) return 1; // countSketch might return negative frequency!
        //return median;
    }

    public int inspectValue(int[] pos, int[] sign) {
        int[] counters = new int[row];
        for (int r = 0; r < row; r++) {
            cs[r * col + pos[r]] += sign[r];
            counters[r] = sign[r] * cs[r * col + pos[r]];
        }

        Arrays.sort(counters, 0, row);
        int median = counters[row / 2];
        if (median <= 0) return 1;
        return median;
    }

/*
    public ImpPair[] getTopK() {
        return topK.getHeap();
    }*/
}
