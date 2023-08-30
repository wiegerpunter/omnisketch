package omniVLDB;

import java.io.*;
import java.util.Arrays;

import static java.lang.Integer.parseInt;

public class RecordSNMP extends Record implements Externalizable {
    //int id;
    //Long[] record = new Long[Main.numAttributes];
    //int ph = -999;
    //long phl = -999;

    public RecordSNMP(Record r) {
        super();
    }

    public RecordSNMP() {
        super();
    }


    public RecordSNMP(long id, long timestamp, long[] record) {
        super();
        this.id = (int) id;
        this.timestamp = timestamp;
        this.record = record;
        this.assembled = true;
    }
    /*
        public Record(int id, String city, String state, String name) {
            this.id = id;
            this.record.add((long) city.hashCode());
            this.record.add((long) state.hashCode());
            this.record.add((long) name.hashCode());
        }*/
    /*public RecordSNMP(int id, Long[] record) {
        this.record = record;
        this.id = id;
    }*/

    /*
    public Record(int id, int city, int state, int name) {
        this.id = id;
        this.record.add((long) city);
        this.record.add((long) state);
        this.record.add((long) name);
    }*/

    // Copy constructor for Record
    /*
    public Record(Record r) {
        this.id = r.id;
        this.record = new ArrayList<Long>(r.record);
    }
*/
    /*
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public Long[] getRecord() {
        return record;
    }

    public String toString() {
        return "ID: " + id + " Record: " + Arrays.toString(record);
    }*/

    long timestamp = phl;
    transient int AP = ph;
    transient long sysUpTime = phl;
    transient int sysDescr = ph;
    public boolean sysAdd(String[] a) {
        if (a.length > 5 || a.length < 4) {
            System.err.println("Warning: sysAdd() in Record.java");
            System.err.println(Arrays.toString(a));
            return false;
            //System.exit(1);
        }
        if (a.length == 5) {
            this.sysDescr = a[4].hashCode();
        }
        for (String s : a) {
            if (s.equals(Integer.toString(ph))) {
                System.err.println("Error found for " + Arrays.toString(a));
                throw new IllegalArgumentException("Warning: c1Add() in Record.java");
            }
        }
        this.timestamp = Long.parseLong(a[1]);
        this.AP = a[2].hashCode();
        this.sysUpTime = Long.parseLong(a[3]);
        return true;
    }

    transient int ifIndex = ph; //keep
    transient int ifDescr = ph; //keep
    transient int ifType = ph; //keep
    transient int ifSpeed = ph;
    transient long ifInOctets = phl; // keep
    transient int ifInUcastPkts = ph;
    transient int ifInErrors = ph;
    transient int ifInDiscards = ph;
    transient long ifOutOctets = phl;
    transient int ifOutUcastPkts = ph;
    transient int ifOutErrors = ph;
    transient int ifOutDiscards = ph;
    transient int awcDot11AssociatedStationCount = ph;
    transient int awcDot11ReassociatedStationCount = ph;
    transient int awcDot11RoamedStationCount = ph;
    transient int awcDot11DeauthenticateCount = ph;
    transient int awcDot11DisassociateCount = ph;
    transient int awcFtClientSTASelf = ph;
    transient int awcFtBridgeSelf = ph;
    transient int awcFtRepeaterSelf = ph;
    public boolean  ifAdd(String[] a) {
        if (a.length != 23) {
            //System.err.println("Error: ifAdd() in Record.java");
            //System.err.println(Arrays.toString(a));
            return false;
            //System.exit(1);
        }
        for (String s : a) {
            if (s.equals(Integer.toString(ph))) {
                System.err.println("Error found for " + Arrays.toString(a));
                throw new IllegalArgumentException("Error: c1Add() in Record.java");
            }
        }
        this.ifIndex = parseInt(a[3]);
        this.ifDescr = a[4].hashCode();
        this.ifType = parseInt(a[5]);
        this.ifSpeed = parseInt(a[6]);
        this.ifInOctets = Long.parseLong(a[7]);
        this.ifInUcastPkts = parseInt(a[8]);
        this.ifInErrors = parseInt(a[9]);
        this.ifInDiscards = parseInt(a[10]);
        this.ifOutOctets = Long.parseLong(a[11]);
        this.ifOutUcastPkts = parseInt(a[12]);
        this.ifOutErrors = parseInt(a[13]);
        this.ifOutDiscards = parseInt(a[14]);
        this.awcDot11AssociatedStationCount = nullHandler(a[15]);
        this.awcDot11ReassociatedStationCount = nullHandler(a[16]);
        this.awcDot11RoamedStationCount = nullHandler(a[17]);
        this.awcDot11DeauthenticateCount = nullHandler(a[18]);
        this.awcDot11DisassociateCount = nullHandler(a[19]);
        this.awcFtClientSTASelf = nullHandler(a[20]);
        this.awcFtBridgeSelf = nullHandler(a[21]);
        this.awcFtRepeaterSelf = parseInt(a[22]);
        return true;
    }

    public int nullHandler(String a) {
        if (a.equals(""))
            return ph;
        else
            return parseInt(a);
    }

    /*
    transient int awcDot11TpFdbAddress = ph;
    transient int awcDot11TpFdbAID = ph;
    transient int awcDot11TpFdbClientState = ph;
    transient int awcDot11TpFdbLatestRxSignalStrength = ph;
    transient int awcDot11TpFdbLatestRxSignalQuality = ph;
    transient int awcDot11TpFdbCurrentBSS = ph;
    transient int awcDot11TpFdbSSID = ph;
    public boolean c1Add(String[] a) {
        if (a.length != 10) {
            //System.err.println("Error: c1Add() in Record.java");
            //System.err.println(Arrays.toString(a));
            return false;
        }
        for (String s : a) {
            if (s.equals(Integer.toString(ph))) {
                System.err.println("Error found for " + Arrays.toString(a));
                throw new IllegalArgumentException("Error: c1Add() in Record.java");
            }
        }
        this.awcDot11TpFdbAddress = a[3].hashCode();
        this.awcDot11TpFdbAID = parseInt(a[4]);
        this.awcDot11TpFdbClientState = parseInt(a[5]);
        this.awcDot11TpFdbLatestRxSignalStrength = parseInt(a[6]);
        this.awcDot11TpFdbLatestRxSignalQuality = parseInt(a[7]);
        this.awcDot11TpFdbCurrentBSS = a[8].hashCode();
        this.awcDot11TpFdbSSID = parseInt(a[9]);
        return true;
    }

    transient int awcTpFdbAddress = ph;
    transient int awcTpFdbClassID = ph;
    transient long awcTpFdbSrcOctetsImmed = phl;
    transient long awcTpFdbDestOctetsImmed = phl;
    transient int awcTpFdbIPv4Addr = ph;
    transient int awcTpFdbSrcPktsImmed = ph;
    transient int awcTpFdbDestPktsImmed = ph;
    transient int awcTpFdbSrcErrorPktsImmed = ph;
    transient int awcTpFdbDestErrorPktsImmed = ph;

    public boolean c2Add(String[] a) {
        if (a.length != 12) {
            //System.err.println("Error: c2Add() in Record.java");
            //System.err.println(Arrays.toString(a));
            return false;
        }
        for (String s : a) {
            if (s.equals(Integer.toString(ph))) {
                System.err.println("Error found for " + Arrays.toString(a));
                throw new IllegalArgumentException("Error: c1Add() in Record.java");
            }
        }
        this.awcTpFdbAddress = a[3].hashCode();
        this.awcTpFdbClassID = parseInt(a[4]);
        this.awcTpFdbSrcOctetsImmed = Long.parseLong(a[5]);
        this.awcTpFdbDestOctetsImmed = Long.parseLong(a[6]);
        this.awcTpFdbIPv4Addr = a[7].hashCode();
        this.awcTpFdbSrcPktsImmed = parseInt(a[8]);
        this.awcTpFdbDestPktsImmed = parseInt(a[9]);
        this.awcTpFdbSrcErrorPktsImmed = parseInt(a[10]);
        this.awcTpFdbDestErrorPktsImmed = parseInt(a[11]);
        return true;
    }
    */


    /*
    public void assemble(int id){

        assembleFirstTwo(id);

        // C1
        this.record.add((long) awcDot11TpFdbAddress);
        this.record.add((long) awcDot11TpFdbAID);
        this.record.add((long) awcDot11TpFdbClientState);
        this.record.add((long) awcDot11TpFdbLatestRxSignalStrength);
        this.record.add((long) awcDot11TpFdbLatestRxSignalQuality);
        this.record.add((long) awcDot11TpFdbCurrentBSS);
        this.record.add((long) awcDot11TpFdbSSID);

        // C2
        this.record.add((long) awcTpFdbAddress);
        this.record.add((long) awcTpFdbClassID);
        this.record.add((long) awcTpFdbSrcOctetsImmed);
        this.record.add((long) awcTpFdbDestOctetsImmed);
        this.record.add((long) awcTpFdbIPv4Addr);
        this.record.add((long) awcTpFdbSrcPktsImmed);
        this.record.add((long) awcTpFdbDestPktsImmed);
        this.record.add((long) awcTpFdbSrcErrorPktsImmed);
        this.record.add((long) awcTpFdbDestErrorPktsImmed);

        this.assembled = true;

    }*/

    @Override
    public void assemble(int id) {
        this.id = id;
        this.record = new long[Main.numAttributes];
        this.record[Parser.attrMap("timestamp")] = timestamp;
        if (Main.numAttributes > 1) {this.record[Parser.attrMap("AP")] = (long) AP;}
        if (Main.numAttributes > 2) {this.record[Parser.attrMap("sysUpTime")] = sysUpTime;}
        if (Main.numAttributes > 3) {this.record[Parser.attrMap("sysDescr")] = (long) sysDescr;}

        // If
        if (Main.numAttributes > 4) {this.record[Parser.attrMap("ifIndex")] = (long) ifIndex;}
        if (Main.numAttributes > 5) {this.record[Parser.attrMap("ifDescr")] = (long) ifDescr;}
        if (Main.numAttributes > 6) {this.record[Parser.attrMap("ifType")] = (long) ifType;}
        if (Main.numAttributes > 7) {this.record[Parser.attrMap("ifSpeed")] = (long) ifSpeed;}
        if (Main.numAttributes > 8) {this.record[Parser.attrMap("ifInOctets")] = ifInOctets;}
        if (Main.numAttributes > 9) {this.record[Parser.attrMap("ifInUcastPkts")] = (long) ifInUcastPkts;}
        if (Main.numAttributes > 10) {this.record[Parser.attrMap("ifInErrors")] = (long) ifInErrors;}
        if (Main.numAttributes > 11) {this.record[Parser.attrMap("ifInDiscards")] = (long) ifInDiscards;}
        if (Main.numAttributes > 12) {this.record[Parser.attrMap("ifOutOctets")] = ifOutOctets;}
        if (Main.numAttributes > 13) {this.record[Parser.attrMap("ifOutUcastPkts")] = (long) ifOutUcastPkts;}
        if (Main.numAttributes > 14) {this.record[Parser.attrMap("ifOutErrors")] = (long) ifOutErrors;}
        if (Main.numAttributes > 15) {this.record[Parser.attrMap("ifOutDiscards")] = (long) ifOutDiscards;}
        if (Main.numAttributes > 16) {this.record[Parser.attrMap("awcDot11AssociatedStationCount")] = (long) awcDot11AssociatedStationCount;}
        if (Main.numAttributes > 17) {this.record[Parser.attrMap("awcDot11ReassociatedStationCount")] = (long) awcDot11ReassociatedStationCount;}
        if (Main.numAttributes > 18) {this.record[Parser.attrMap("awcDot11RoamedStationCount")] = (long) awcDot11RoamedStationCount;}
        if (Main.numAttributes > 19) {this.record[Parser.attrMap("awcDot11DeauthenticateCount")] = (long) awcDot11DeauthenticateCount;}
        if (Main.numAttributes > 20) {this.record[Parser.attrMap("awcDot11DisassociateCount")] = (long) awcDot11DisassociateCount;}
        if (Main.numAttributes > 21) {this.record[Parser.attrMap("awcFtClientSTASelf")] = (long) awcFtClientSTASelf;}
        if (Main.numAttributes > 22) {this.record[Parser.attrMap("awcFtBridgeSelf")] = (long) awcFtBridgeSelf;}
        if (Main.numAttributes > 23) {this.record[Parser.attrMap("awcFtRepeaterSelf")] = (long) awcFtRepeaterSelf;}
        // assess if this.record has no values equal to -999:
        this.assembled = true;
    }

    boolean seenSys = true;
    boolean seenIf = true;
    boolean seenC1 = true;
    boolean seenC2 = true;


    @Override public void add(String[] a) {
        //System.out.println("Record.add() " + a[0]);
        if (a[0].equals("sys")) {
            seenSys =  sysAdd(a);
        } else if (a[0].equals("if") && seenSys) {
            seenIf = ifAdd(a);
        } /*else if (a[0].equals("c1") && seenIf && seenSys) {
            seenC1 = c1Add(a);
        } else if (a[0].equals("c2") && seenIf && seenSys && seenC1) {
            seenC2 =  c2Add(a);
        } */else if (!('#' == a[0].charAt(0) || 's' == a[0].charAt(0) || 'i' == a[0].charAt(0) || 'c' == a[0].charAt(0))) {
            System.err.println("Unknown record type " + a[0]);
        }
        if (seenSys && seenIf) {
            assemble(id);
        }
        /*
        if (seenSys && seenIf && seenC1 && seenC2) {
            assemble(id);
            throw new RuntimeException("Record assembled");
        } else if (seenSys && seenIf && !seenC1 && !seenC2) {
            assemble(id);
        }*/
    }

    public String[] writeRecord() {
        String[] info = new String[Main.numAttributes + 2];
        info[0] = String.valueOf(id);
        info[1] = String.valueOf(timestamp);
        for (int i = 0; i < Main.numAttributes; i++) {
            info[i + 2] = String.valueOf(record[i]);
        }
        return info;
    }


    //public boolean isQuery = false;
    //public ArrayList<Integer> predAttrs = new ArrayList<Integer>();
/*
    public void setQuery() {
        this.isQuery = true;
        for (int i = 0; i < Main.numAttributes; i++)
            if (record[i] != phl) {
                if (record[i] != ph)
                    this.predAttrs.add(i);
            }
                //this.predAttrs.add(i);
    }
*/
    /*
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);
        out.writeObject(record);
        out.writeLong(timestamp);
        //out.writeBoolean(isQuery);
        //out.writeObject(predAttrs);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = in.readInt();
        this.record = (Long[]) in.readObject();
        this.timestamp = in.readLong();
        //this.isQuery = in.readBoolean();
        //this.predAttrs = (ArrayList<Integer>) in.readObject();
    }*/
}
