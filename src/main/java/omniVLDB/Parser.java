package omniVLDB;

import java.util.ArrayList;
import java.util.Arrays;

public class Parser {
    public static String[] getAttributeName;
    // Class processing queries to Query objects
    // Method mapping attribute name to attribute index

    public int queryId;
    public Parser(int queryId) {
        this.queryId = queryId;
    }

    public static ArrayList<String> ordinalAttributesSNMP = new ArrayList<String>(Arrays.asList("sysUpTime", "ifSpeed", "ifInOctets", "ifInUcastPkts", "ifInErrors",
            "ifInDiscards", "ifOutOctets", "ifOutUcastPkts", "ifOutErrors", "ifOutDiscards",
            "awcDot11AssociatedStationCount", "awcDot11ReassociatedStationCount", "awcDot11RoamedStationCount",
            "awcDot11DeauthenticateCount", "awcDot11DisassociateCount", "awcFtClientSTASelf", "awcFtBridgeSelf", "awcFtRepeaterSelf"));

    public static ArrayList<String> ordinalAttributesCAIDA = new ArrayList<String>(Arrays.asList("timestamp"));
    public static int attrMap(String attrName) {
        if (Main.datasetName.equals("SNMP")) {

            return switch (attrName) {
                case "timestamp" -> 0;
                case "AP" -> 1;
                case "sysUpTime" -> 2;
                case "sysDescr" -> 3;
                case "ifIndex" -> 4;
                case "ifDescr" -> 5;
                case "ifType" -> 6;
                case "ifSpeed" -> 7;
                case "ifInOctets" -> 8;
                case "ifInUcastPkts" -> 9;
                case "ifInErrors" -> 10;
                case "ifInDiscards" -> 11;
                case "ifOutOctets" -> 12;
                case "ifOutUcastPkts" -> 13;
                case "ifOutErrors" -> 14;
                case "ifOutDiscards" -> 15;
                case "awcDot11AssociatedStationCount" -> 16;
                case "awcDot11ReassociatedStationCount" -> 17;
                case "awcDot11RoamedStationCount" -> 18;
                case "awcDot11DeauthenticateCount" -> 19;
                case "awcDot11DisassociateCount" -> 20;
                case "awcFtClientSTASelf" -> 21;
                case "awcFtBridgeSelf" -> 22;
                case "awcFtRepeaterSelf" -> 23;
                default -> throw new IllegalArgumentException("Unknown attribute name: " + attrName);
            };
        } else if (Main.datasetName.equals("CAIDA")) {
            return switch (attrName) {
                case "timestamp" -> 0;
                case "frameNumber" -> 1;
                case "ethSrc" -> 2;
                case "ethDst" -> 3;
                case "ipSrc" -> 4;
                case "ipSrcNet" -> 5;
                case "ipSrcHost" -> 6;
                case "ipDst" -> 7;
                case "ipDstNet" -> 8;
                case "ipDstHost" -> 9;
                case "ipProto" -> 10;
                default -> throw new IllegalArgumentException("Unknown attribute name: " + attrName);
            };
        } else {
            return switch (attrName) {
                case "0" -> 0;
                case "1" -> 1;
                case "2" -> 2;
                case "3" -> 3;
                case "4" -> 4;
                default -> throw new IllegalArgumentException("Unknown attribute name: " + attrName);
            };
        }
    }

    // Also dependent on datasetName
    static String[] attributeNamesSNMP = new String[]{"timestamp", "AP", "sysUpTime", "sysDescr",
            "ifIndex", "ifDescr", "ifType", "ifSpeed", "ifInOctets",
            "ifInUcastPkts", "ifInErrors", "ifInDiscards", "ifOutOctets",
            "ifOutUcastPkts", "ifOutErrors", "ifOutDiscards", "awcDot11AssociatedStationCount",
            "awcDot11ReassociatedStationCount", "awcDot11RoamedStationCount", "awcDot11DeauthenticateCount",
            "awcDot11DisassociateCount", "awcFtClientSTASelf", "awcFtBridgeSelf", "awcFtRepeaterSelf"};

    static String[] attributeNamesCAIDA = new String[]{"timestamp", "frameNumber", "ethSrc", "ethDst", "ipSrc", "ipSrcNet", "ipSrcHost", "ipDst", "ipDstNet", "ipDstHost", "ipProto"};
    static String[] attributeNamesSynth = new String[]{"0", "1", "2", "3", "4"};

    public static String getAttributeName(int i) {
        if (Main.datasetName.equals("SNMP")) {
            return attributeNamesSNMP[i];
        } else if (Main.datasetName.equals("CAIDA")) {
            return attributeNamesCAIDA[i];
        } else {
            return attributeNamesSynth[i];
        }
    }

    public void setPredicate(Query q, String predicate) {
        // Parse query string into Query object
        // Query string format: "attrName op value"
        // Example: "ifInOctets > 1000"
        String[] queryParts = predicate.split(" ");
        String attrName = queryParts[0];
        String op = queryParts[1];
        String value = queryParts[2];
        int attrIndex = attrMap(attrName);
        q.addPredicate(attrIndex, op, value);
    }

    public Query parse(String query) {
        // Example: "timestamp > 1000 AND ifInOctets > 1000"
        String[] queryParts = query.split(" (A|a)(N|n)(D|d) ");
        Query q = new Query(queryId);
        q.predicates = query;
        for (String queryPart : queryParts) {
            if (!queryPart.equals("AND")) {
                setPredicate(q, queryPart);
            }
        }
        return q;
    }

}
