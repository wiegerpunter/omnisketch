package omniVLDB;

import java.io.Externalizable;

public class RecordCAIDA extends Record implements Externalizable {

    public RecordCAIDA() {
        super();
    }

    public RecordCAIDA(Record r) {
        super();
    }

    int frameNumber;
    //long frameTime;
    int ethSrc = ph;
    int ethDst = ph;
    String ipSrc = "";
    String ipDst = "";
    int ipSrcNet;
    int ipSrcHost;
    int ipDstNet;
    int ipDstHost;
    int ipProto = ph;


    @Override public void add(String[] a) {
        //String frameNumberString = a[0].replaceAll("\"", "");
        frameNumber = Integer.parseInt(a[0].replaceAll("\"", ""));


        timestamp = convertFrameTime(a[1].replaceAll("\"", ""), a[2].replaceAll("\"", ""));
        if (a.length < 4) {
            return;
        }
        if (a[3].equals("")) {
            ethSrc = ph;
        } else {
            ethSrc = Integer.parseInt(a[3].replaceAll("\"", ""));
        }
        if (a[4].equals("")) {
            ethDst = ph;
        } else {
            ethDst = Integer.parseInt(a[4].replaceAll("\"", ""));
        }
        ipSrc = a[5].replaceAll("\"", "");
        //int secDot = (ipSrc.substring(ipSrc.indexOf("."))).indexOf(".");
        if (ipSrc != "") {

            int firstDot = ipSrc.indexOf(".");
            int secondDot = ipSrc.indexOf(".", firstDot + 1);
            ipSrcNet = Integer.parseInt(ipSrc.substring(0, secondDot).replaceAll("\\.", ""));
            ipSrcHost = Integer.parseInt(ipSrc.substring(secondDot).replaceAll("\\.", ""));
        }
        ipDst = a[6].replaceAll("\"", "");
        if (ipDst  != ""){
            int firstDot = ipDst.indexOf(".");
            int secondDot = ipDst.indexOf(".", firstDot + 1);
            ipDstNet = Integer.parseInt(ipDst.substring(0, secondDot).replaceAll("\\.", ""));
            ipDstHost = Integer.parseInt(ipDst.substring(secondDot).replaceAll("\\.", ""));
        }
        ipProto = Integer.parseInt(a[7].replaceAll("\"", ""));
    }

    @Override public void assemble(int id) {
        this.id = id;
        record[0] = timestamp;
        if (Main.numAttributes > 1) {record[1] = (long) frameNumber;}
        if (Main.numAttributes > 2) {record[2] = (long) ethSrc;}
        if (Main.numAttributes > 3) {record[3] = (long) ethDst;}
        if (Main.numAttributes > 4) {
            if (ipSrc.equals("")) {
                record[4] = (long) ph;
            } else {
                record[4] = (long) Long.parseLong(ipSrc.replaceAll("\\.", ""));//.hashCode();}
            }
        }
        if (Main.numAttributes > 5) {record[5] = (long) ipSrcNet;}
        if (Main.numAttributes > 6) {record[6] = (long) ipSrcHost;}
        if (Main.numAttributes > 7) {
            if (ipDst.equals("")) {
                record[7] = phl;
            } else {
                record[7] = (long) Long.parseLong(ipDst.replaceAll("\\.", ""));}
        }
        if (Main.numAttributes > 8) {record[8] = (long) ipDstNet;}
        if (Main.numAttributes > 9) {record[9] = (long) ipDstHost;}
        if (Main.numAttributes > 10) {record[10] = (long) ipProto;}
        assembled = true;
    }


     /*
    int ipTos;
    int ipTtl;
    int ipLen;
    int ipId;
    int ipFlags;
    int ipFragOffset;
    int tcpSrcPort;
    int tcpDstPort;
    int tcpSeq;
    int tcpAck;
    int tcpLen;
    int tcpFlags;
    int tcpWin;
    int tcpUrg;
    int tcpOptions;
    int tcpOptionsLen;
    int udpSrcPort;
    int udpDstPort;
    int udpLen;
    int udpChecksum;
    int icmpType;
    int icmpCode;
    int icmpChecksum;
    int icmpId;
    int icmpSeq;
    int icmpDataLen;
    int icmpData;
     */

    public long convertFrameTime(String a1, String a2) {
        // Converts string in format feb 17 2011 13:59:04.112376000 CET" to timestamp
        String[] b = a1.split(" ");
        String month = b[0];
        int day = Integer.parseInt(b[1]);
        String[] c = a2.split(" ");
        int year = Integer.parseInt(c[1]);
        String[] d = c[2].split(":");
        int hour = Integer.parseInt(d[0]);
        int minute = Integer.parseInt(d[1]);
        String[] e = d[2].split("\\.");
        int second = Integer.parseInt(e[0]);
        int millisecond = Integer.parseInt(e[1]);
        String zone = c[3];

        String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
        int monthInt = 0;
        for (int x = 0; x < months.length; x++) {
            if (months[x].equals(month)) {
                monthInt = x + 1;
                break;
            }
        }
        return (year * 10000000000L) + (monthInt * 100000000L) + (day * 1000000L) + (hour * 10000L) + (minute * 100L) + second;
    }
}
