package omniVLDB;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;

public class Query extends Record {

    public long exactTime;
    public String predicates;

    //Long[] record = new Long[Main.numAttributes];
    public long[] lower = new long[Main.numAttributes];
    public long[] upper = new long[Main.numAttributes];

    public int idxHighestAttr = Main.numAttributes - 1;

    public boolean rangeQuery = false;
    public Query(int id) {
        this.id = id;
    }

    public ArrayList<Integer> predAttrs = new ArrayList<>();

    public boolean[] pointAttrs = new boolean[Main.numAttributes];
    public ArrayList<Boolean> rangeAttrs = new ArrayList<Boolean>();

    public Query(int id, long[] record) {
        this.id = id;
        this.record = record;
    }

    public Query() {
    }

    public Query(String[] nextLine) {
        this.id = Integer.parseInt(nextLine[0]);
        this.predicates = nextLine[2];
        this.predAttrs = parseArrListInt(nextLine[3]);
        this.numPredicates = this.predAttrs.size();
        this.pointAttrs = parseBoolArr(nextLine[4]);
        this.rangeQuery = Boolean.parseBoolean(nextLine[5]);
        this.rangeAttrs = parseArrListBool(nextLine[6]);
        this.idxHighestAttr = Integer.parseInt(nextLine[7]);
        this.exactAnswer = Double.parseDouble(nextLine[8]);
        this.exactTime = Long.parseLong(nextLine[9]);
        this.queryBin = Integer.parseInt(nextLine[10]);
        this.record = parseLongArr(nextLine[11]);
        if (nextLine.length > 12) {
            this.lower = parseLongArr(nextLine[12]);
            this.upper = parseLongArr(nextLine[13]);
        }
    }

    public String[] getQueryInfo() {
        String[] queryInfo = new String[12];
        queryInfo[0] = String.valueOf(id);
        queryInfo[1] = String.valueOf(numPredicates);
        queryInfo[2] = predicates;
        queryInfo[3] = predAttrs.toString();
        queryInfo[4] = Arrays.toString(pointAttrs);
        queryInfo[5] = String.valueOf(rangeQuery);
        queryInfo[6] = rangeAttrs.toString();
        queryInfo[7] = String.valueOf(idxHighestAttr);
        queryInfo[8] = String.valueOf(exactAnswer);
        queryInfo[9] = String.valueOf(exactTime);
        queryInfo[10] = String.valueOf(queryBin);
        queryInfo[11] = Arrays.toString(record);
        return queryInfo;
    }

    public String[] getQueryInfoRange() {
        String[] queryInfo = new String[14];
        queryInfo[0] = String.valueOf(id);
        queryInfo[1] = String.valueOf(numPredicates);
        queryInfo[2] = predicates;
        queryInfo[3] = predAttrs.toString();
        queryInfo[4] = Arrays.toString(pointAttrs);
        queryInfo[5] = String.valueOf(rangeQuery);
        queryInfo[6] = rangeAttrs.toString();
        queryInfo[7] = String.valueOf(idxHighestAttr);
        queryInfo[8] = String.valueOf(exactAnswer);
        queryInfo[9] = String.valueOf(exactTime);
        queryInfo[10] = String.valueOf(queryBin);
        queryInfo[11] = Arrays.toString(record);
        queryInfo[12] = Arrays.toString(lower);
        queryInfo[13] = Arrays.toString(upper);
        return queryInfo;
    }

    private ArrayList<Integer> parseArrListInt(String infoString) {
        infoString = infoString.replaceAll("\\[|\\]", ""); // Remove square brackets
        String[] values = infoString.split(",");
        ArrayList<Integer> info = new ArrayList<>();

        for (String value : values) {
            info.add(Integer.parseInt(value.trim()));
        }
        return info;
    }

    private ArrayList<Boolean> parseArrListBool(String infoString) {
        infoString = infoString.replaceAll("\\[|\\]", ""); // Remove square brackets
        String[] values = infoString.split(",");
        ArrayList<Boolean> info = new ArrayList<>();

        for (String value : values) {
            info.add(Boolean.parseBoolean(value.trim()));
        }
        return info;
    }

    private boolean[] parseBoolArr(String infoString) {
        infoString = infoString.replaceAll("\\[|\\]", ""); // Remove square brackets
        String[] values = infoString.split(",");
        boolean[] info = new boolean[values.length];

        for (int i = 0; i < values.length; i++) {
            info[i] = Boolean.parseBoolean(values[i].trim());
        }
        return info;
    }

    private long[] parseLongArr(String infoString) {
        infoString = infoString.replaceAll("\\[|\\]", ""); // Remove square brackets
        String[] values = infoString.split(",");
        long[] info = new long[values.length];

        for (int i = 0; i < values.length; i++) {
            info[i] = Long.parseLong(values[i].trim());
        }
        return info;
    }





    public void assemble(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public void addPredicate(int attrIndex, String op, String value) {
        // if attrIndex is categorical, we hash the value
        /*if (Arrays.stream(Parser.getCatAttr()).anyMatch(x -> x == attrIndex)) {
            add(attrIndex, value.hashCode());
            return;
        }*/
        switch (op) {
            case "=" -> add(attrIndex, Long.parseLong(value));
            case ">" -> addRange(attrIndex, Long.parseLong(value) + 1, Long.MAX_VALUE);
            case "<" -> addRange(attrIndex, 0, Long.parseLong(value) - 1);
            case ">=" -> addRange(attrIndex, Long.parseLong(value), Long.MAX_VALUE);
            case "<=" -> addRange(attrIndex, 0, Long.parseLong(value));
            case "in" -> {
                String[] values = value.split(",");
                long min = Long.parseLong(values[0]);
                long max = Long.parseLong(values[1]);
                addRange(attrIndex, min, max);
            }
            case "!=" -> //TODO: check if we want to support this query operator;
                //addRange(attrIndex, Long.MIN_VALUE, Long.parseLong(value) - 1);
                //addRange(attrIndex, Long.parseLong(value) + 1, Long.MAX_VALUE);
                    throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public void add(int attr, long value) {
        this.record[attr] = value;
        this.pointAttrs[attr] = true;
        this.predAttrs.add(attr);
        this.rangeAttrs.add(false);
    }

    public void addRange(int attr, long min, long max) {
        this.rangeQuery = true;
        this.predAttrs.add(attr);
        this.rangeAttrs.add(true);
        this.lower[attr] = min;
        this.upper[attr] = max;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(idxHighestAttr);
        out.writeObject(record);
        out.writeLong(timestamp);
        //out.writeBoolean(isQuery);
        out.writeObject(predAttrs);
        out.writeObject(pointAttrs);
        out.writeObject(rangeAttrs);
        out.writeObject(lower);
        out.writeObject(upper);
        out.writeObject(predicates);
        out.writeDouble(exactAnswer);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = in.readInt();
        this.idxHighestAttr = in.readInt();
        this.record = (long[]) in.readObject();
        this.timestamp = in.readLong();
        //this.isQuery = in.readBoolean();
        this.predAttrs = (ArrayList<Integer>) in.readObject();
        this.pointAttrs = (boolean[]) in.readObject();
        this.rangeAttrs = (ArrayList<Boolean>) in.readObject();
        this.lower = (long[]) in.readObject();
        this.upper = (long[]) in.readObject();
        this.predicates = (String) in.readObject();
        this.exactAnswer = in.readDouble();
    }

    public boolean pointQueryEqual(Query other) {
        if (this.predAttrs.size() != other.predAttrs.size()) {
            //System.out.println("Query " + this.id + " and " + other.id + " have different number of predicates");
            return false;
        }
        return this.predicates.equals(other.predicates);
    }

    public boolean rangeQueryEqual(Query other) {
        if (this.predAttrs.size() != other.predAttrs.size()) {
            //System.out.println("Query " + this.id + " and " + other.id + " have different number of predicates");
            return false;
        }
        return this.predicates.equals(other.predicates);
    }

    public long[] getLower() {
        return lower;
    }

    public long[] getUpper() {
        return upper;
    }

    // RESULTS TO QUERY

    public double exactAnswer;
    public double estimate;
    public int numPredicates;// = predAttrs.size();
    public double absError;
    public double relError;
    public double epsError;
    public boolean withinTreshold;
    public boolean withinTightBound;
    public long execTime;
    public double intersectSize;
    public int sampleLevel;

    public void setEpsError(int size) {
        this.epsError = absError / size;
    }

    public void setResult(boolean withinBound) {
        this.numPredicates = predAttrs.size();
        this.absError = Math.abs(exactAnswer - estimate);
        this.relError = Math.abs(exactAnswer - estimate) / exactAnswer;
        this.withinTreshold = withinBound;
    }

    double bound;
    double tightBound;

    public void setBound(double bound) {
        this.bound = bound;
    }
    public boolean thrm33Case2 = false;
    public double case2Estimate;
    public boolean ratioCondition = false;
    public int queryBin = -1;

    public String[] getQueryResult() {
        String[] queryResult = new String[19];
        queryResult[0] = String.valueOf(id);
        queryResult[1] = String.valueOf(numPredicates);
        queryResult[2] = String.valueOf(exactAnswer);
        queryResult[3] = String.valueOf(estimate);
        queryResult[4] = String.valueOf(absError);
        queryResult[5] = String.format("%.4f",relError);
        queryResult[6] = String.format("%.4f", epsError);
        queryResult[7] = String.valueOf(withinTreshold);
        queryResult[8] = String.valueOf(execTime);
        queryResult[9] = predicates;
        queryResult[10] = String.valueOf(exactTime);
        queryResult[11] = String.valueOf(intersectSize);
        queryResult[12] = String.valueOf(bound);
        queryResult[13] = String.valueOf(thrm33Case2);
        queryResult[14] = String.valueOf(case2Estimate);
        queryResult[15] = String.valueOf(0);
        queryResult[16] = String.valueOf(ratioCondition);
        queryResult[17] = String.valueOf(queryBin);
        queryResult[18] = String.valueOf(upper[0] - lower[0]);

        return queryResult;
    }

}
