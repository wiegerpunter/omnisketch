package omniVLDB;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class SubPopMap {
    boolean[] attrs;
    boolean[] attrsInWorkload;
    ArrayList<Integer> attrsIdx;
    int numAttrs;
    private AttributeStats[] dstats;


    public SubPopMap(int numAttributes) {
        if (Main.datasetName.equals("SNMP")) {
            this.attrsIdx = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ,12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23));
        } else if (Main.datasetName.equals("CAIDA")) {
            this.attrsIdx = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        } else {
            this.attrsIdx = new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4));
        }
        this.numAttrs= numAttributes;
        initSubPop();
    }
    public SubPopMap(ArrayList<Integer> attrsIdx) {
        this.attrsIdx = attrsIdx;
        this.numAttrs= attrsIdx.size();
        initSubPop();
    }

    public LinkedHashMap<boolean[], Integer> subPopulationsReverse = new LinkedHashMap<>();
    public LinkedHashMap<Integer, ArrayList<Integer>> subPopulations2 = new LinkedHashMap<>();

    LinkedHashMap<Integer, Boolean> subPopOneAttr = new LinkedHashMap<>();

    public HashMap<Integer, Boolean> validSubPopMap = new HashMap<>();
    public int cnt = 0;
    public LinkedHashMap<Integer, LinkedHashMap<Integer, ArrayList<Integer>>> perPredMap = new LinkedHashMap<>();
    public void initSubPop() {

        generateCombinations(attrsIdx, new ArrayList<>(), 0);
        subPopulations2.remove(0);


         perPredMap = splitLinkedHashMap(subPopulations2, attrsIdx.size());
    }
    public void generateCombinations(ArrayList<Integer> attrsIdx, ArrayList<Integer> currentCombination, int start) {
        // Print the current combination
        subPopulations2.put(cnt, currentCombination);
        cnt++;

        ArrayList<Integer> newCombination = new ArrayList<Integer>(currentCombination);
        // Recursively generate combinations with the remaining elements
        for (int i = start; i < attrsIdx.size(); i++) {
            newCombination.add(attrsIdx.get(i));
            generateCombinations(attrsIdx, new ArrayList<>(newCombination), i + 1);
            newCombination.remove(newCombination.size() - 1);
        }
    }


//    public void initSubPop() {
//
////        for (int i = 0; i < Main.numAttributes; i++) {
////            boolean[] attrs = new boolean[Main.numAttributes];
////            for (int k = 0; k < Main.numAttributes; k++) {
////                attrs[k] = false;
////            }
////            attrs[i] = true;
////            subPopOneAttr.put(cnt, true);
////            subPopulations.put(cnt, attrs);
////            cnt++;
////            if (i >= Main.numAttributes - 1) {
////                continue;
////            }
////            recurSubPopInit(i, attrs);
////        }
////        if (cnt != subPopulations.size()) {
////            throw new RuntimeException("cnt != CMSketches.length");
////        }
////        for (int subPop : subPopulations.keySet()) {
////            for (int attr_idx = 0; attr_idx < subPopulations.get(subPop).length; attr_idx++) {
////                boolean[] attrs = subPopulations.get(subPop);
////                for (int i = 0; i < Main.numAttributes; i++) {
////                    if (attrs[i] && !Main.createNewWorkload && !attrsInWorkload[i] ) {
////                        validSubPopMap.put(subPop, false);
////                        break;
////                    }
////                }
////                validSubPopMap.put(subPop, true);
////            }
////        }
//
//        for (int i = 0; i < Main.numAttributes; i++) {
//            boolean[] attrs = new boolean[Main.numAttributes];
//            for (int k = 0; k < Main.numAttributes; k++) {
//                attrs[k] = false;
//            }
//            attrs[i] = true;
//            subPopOneAttr.put(cnt, true);
//            subPopulations.put(cnt, attrs);
//            cnt++;
//            if (i >= Main.numAttributes - 1) {
//                continue;
//            }
//            recurSubPopInit(i, attrs);
//        }
//        if (cnt != subPopulations.size()) {
//            throw new RuntimeException("cnt != CMSketches.length");
//        }
//        for (int subPop : subPopulations.keySet()) {
//            boolean valSubPop = true;
//            for (int attr_idx = 0; attr_idx < subPopulations.get(subPop).length; attr_idx++) {
//                boolean[] attrs = subPopulations.get(subPop);
//                for (int i = 0; i < Main.numAttributes; i++) {
//                    if (attrs[i] && !Main.createNewWorkload && !attrsInWorkload[i] ) {
//                        valSubPop = false;
//                        break;
//                    }
//                }
//                validSubPopMap.put(subPop, valSubPop);
//            }
//        }
//        //Make reverse map
//        for (int subPop : subPopulations.keySet()) {
//            subPopulationsReverse.put(subPopulations.get(subPop), subPop);
//        }
//
//    }
//
//    public void recurSubPopInit(int i, boolean[] attrs) {
//        for (int j = i + 1; j < Main.numAttributes; j++) {
//            boolean[] attrsCopy;
//            attrsCopy = attrs.clone();
//            attrsCopy[j] = true;
//            subPopulations.put(cnt, attrsCopy);
//            cnt++;
//            if (i == Main.numAttributes - 1) {
//                return;
//            }
//            recurSubPopInit(j, attrsCopy);
//        }
//
//    }
//
//    public ArrayList<boolean[]> getValidSubPopulations() {
//        // get all subpopulations that are valid
//        ArrayList<boolean[]> validSubPopulations = new ArrayList<>();
//        for (int subPop : subPopulations.keySet()) {
//            if (validSubPopMap.get(subPop)) {
//                validSubPopulations.add(subPopulations.get(subPop));
//            }
//        }
//        return validSubPopulations;
//    }


    public int getSubPop(ArrayList<Integer> attrsList) {

        for (int subPop : subPopulations2.keySet()) {
            if (attrsList.equals(subPopulations2.get(subPop))) { // Equals works as they are all sorted.
                return subPop;
            }
        }
        throw new RuntimeException("subPop not found");
    }

    public void dstats(AttributeStats[] dStats) {
        this.dstats = dStats;
    }



    public LinkedHashMap<Integer, LinkedHashMap<Integer, ArrayList<Integer>>> splitLinkedHashMap(LinkedHashMap<Integer, ArrayList<Integer>> originalMap, int numAttributes) {
        LinkedHashMap<Integer, LinkedHashMap<Integer, ArrayList<Integer>>> result = new LinkedHashMap<>();

        for (int length = 1; length < numAttributes; length++) {
            LinkedHashMap<Integer, ArrayList<Integer>> newLinkedHashMap = new LinkedHashMap<>();
            result.put(length, newLinkedHashMap);
        }

        for (Integer entry : originalMap.keySet()) {
            int length = originalMap.get(entry).size();
            if (length < numAttributes) {
                result.get(length).put(entry, originalMap.get(entry));
            }
        }

        return result;
    }


}
