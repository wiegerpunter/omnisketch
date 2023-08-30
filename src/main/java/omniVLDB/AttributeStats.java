package omniVLDB;

import java.util.Set;

public class AttributeStats {
    public boolean type;
    public long mostFrequent;
    public long secFrequent;
    public long firstQuantile;
    public long median;
    public long thirdQuantile;
    public Set<Long> values;

    boolean categorical;

    // Stats per attribute
    public AttributeStats(int i) {
        attributeIndex = i;
        attributeName = Parser.getAttributeName(i);
    }
    
    public AttributeStats(String name) {
        attributeName = name;
        attributeIndex = Parser.attrMap(name);
    }
    
    String attributeName;
    int attributeIndex;

    int cardinality;

    public void setValues(Set<Long> values) {
        this.values = values;
    }
    public void setCategoricalStats(long cardinality, long mostFrequent) {
        this.cardinality = (int) cardinality;
        this.mostFrequent = mostFrequent;
        //this.secFrequent = secFrequent;
    }
    
    public int getCardinality() {
        return cardinality;
    }
    
    public boolean isCategorical() {
        return categorical;
    }

    long min;
    long max;
    double avg;
    double variance;
    double stdDev;
    public void setNumericalStats(long min, long max, double avg, double variance, long firstQuantile, long median, long thirdQuantile) {
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.variance = variance;
        this.stdDev = Math.sqrt(variance);
        this.median = median;
        this.firstQuantile = firstQuantile;
        this.thirdQuantile = thirdQuantile;
    }

    public boolean sanityCheckRange(long minQueryRange, long maxQueryRange) {
        if (categorical) {
            return cardinality > 0;
        } else {
            // Constraint on stdev
            //constraint on min <= max
            // constraint on attrName
            if (maxQueryRange - minQueryRange >= 1000) return false;
            if (stdDev <= 10) return false;
            if (avg <= 0) throw new RuntimeException("Avg is negative");
            // attribute name should not contain time, lower case or upper case
            if (attributeName.contains("time")) return false;
            if (attributeName.contains("Time")) return false;

            return min <= max;
        }
    }

    public boolean dontIncludeAttribute(StringBuilder sb) {
        if (attributeName.contains("sysUpTime")) return true;
        if (attributeName.contains("ifIndex")) return true;
        if (attributeName.contains("sysDescr")) return true;
        if (attributeName.contains("ifType")) return true;
        if (attributeName.contains("ifSpeed")) return true;
        if (attributeName.contains("awcFtBridgeSelf")) return true;
        if (attributeName.contains("awcFtRepeaterSelf")) return true;
        if (attributeName.contains("awcDot11ReassociatedStationCount")) return true;
        if (attributeName.contains("awcDot11RoamedStationCount")) return true;
        if (attributeName.contains("awcDot11DeauthenticateCount")) return true;
        if (attributeName.contains("awcDot11DisassociateCount")) return true;


        //"awcDot11AssociatedStationCount" -> 16;
        //                case "awcDot11ReassociatedStationCount" -> 17;
        //                case "awcDot11RoamedStationCount" -> 18;
        //                case "awcDot11DeauthenticateCount" -> 19;
        //                case "awcDot11DisassociateCount" -> 20;
//        Parser.attrMap("sysUpTime"),
//                Parser.attrMap("ifIndex"),
//                Parser.attrMap("ifType"),
//                Parser.attrMap("ifSpeed"),
//                Parser.attrMap("awcFtBridgeSelf"),
//                Parser.attrMap("awcFtRepeaterSelf")
        if (attributeName.contains("ethSrc")) return true;
        if (attributeName.contains("timestamp")) return true;
        if (attributeName.contains("frameNumber")) return true;
        if (sb.toString().contains("ipSrc ")){
            if (attributeName.contains("ipSrcNet") || attributeName.contains("ipSrcHost")) return true;
        }
        if (sb.toString().contains("ipSrcNet") || sb.toString().contains("ipSrcHost")){
            if (attributeName.contains("ipSrc")) return true;
        }
        if (sb.toString().contains("ipSrc =")){
            if (attributeName.contains("ipSrcNet") || attributeName.contains("ipSrcHost")) return true;
        }
        if (sb.toString().contains("ipDstNet") || sb.toString().contains("ipDstHost")){
            if (attributeName.contains("ipDst")) return true;
        }
        return attributeName.contains("ethDst");
        /*if (categorical) {
            if (mostFrequent == -999) return true;
            return cardinality <= 0;
        } else {
            //if (stdDev <= 1) return false;
            //if (attributeName.contains("time")) return true;

            //if (attributeName.contains("ifType")) return true;
            //if (attributeName.contains("ifSpeed")) return true;
            //if (attributeName.contains("awcFtBridgeSelf")) return true;
            //if (attributeName.contains("awcFtRepeaterSelf")) return true;
        }*/
    }
}

