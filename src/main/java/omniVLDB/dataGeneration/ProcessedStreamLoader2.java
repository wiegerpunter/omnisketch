package omniVLDB.dataGeneration;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import omniVLDB.structure.logEventInt;



public class ProcessedStreamLoader2 {
	long lastTime=-1;
	long starttime=-1;
	static String pattern = "wc_day";
	String filename;
	int dayno=1,fileno=1;
	boolean fileExists=true;
	String currentFilename;
	BufferedReader br;
	long line=0;
	final double ratio;
	final int throwAwayLines;
	public void reset() {
		this.dayno=1;
		this.fileno=1;
		openFile(filename);
	}
	public void reset2() {
		this.dayno=1;
		this.fileno=1;
		openFile2(filename);
	}
	
	public void openFile(String filename) {
		try {
			if (br!=null) br.close();
		} catch (Exception ignored){}
		try {
			System.err.println("Opening file " + filename);
			FileChannel fc = new RandomAccessFile(filename, "r").getChannel();
			GZIPInputStream gzipinput = new GZIPInputStream(new BufferedInputStream(Channels.newInputStream(fc), 1024*8192));
			br = new BufferedReader(new InputStreamReader(gzipinput));
		} catch (Exception e){e.printStackTrace();}
	}
	
	public void openFile2(String filename) {
		try {
			if (br!=null) br.close();
		} catch (Exception ignored){}
		try {
			System.err.println("Opening file " + filename);
			GZIPInputStream gzipinput = new GZIPInputStream(new BufferedInputStream(new FileInputStream(filename), 1024*8192));
			br = new BufferedReader(new InputStreamReader(gzipinput));
		} catch (Exception e){e.printStackTrace();}
	}
	
	HashMap<String, Integer> streamids=new HashMap<String, Integer>();

	static Random rn = new Random();
	
	final short tmp=0;

	public HashMap<String, Integer> macs = new HashMap<String, Integer>(10000);
	public logEventInt readNextLineInt2() {
		try {
			if (!br.ready())
				return null;
			else {
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready()) 
					return null;
				line++;
				String[] brokenline = br.readLine().split(",");
				int time=Integer.parseInt(brokenline[1])-1067662864;
				String serverid = brokenline[2];
				Integer sid;
				if ((sid=streamids.get(serverid))==null) {
					sid=streamids.size()+1;
					streamids.put(serverid,sid);
				}
	
				Integer val = macs.get(brokenline[3]);
				if (val==null) {
					val=macs.size()+1;
					macs.put(brokenline[3], val);
				}
				int ipaddress = val;
//				int ipaddress = Math.abs(brokenline[3].hashCode());
				int filename = ipaddress;

				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				if (this.numberOfStreams>0)
					return new logEventInt(ipaddress, time, filename, (int)((time/1000)%numberOfStreams));
				else
					return new logEventInt(ipaddress, time, filename, sid-1); // streamid in files starts from 0
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}
	
	Random rn2 = new Random(123);
	
	final int numberOfStreams;
	public ProcessedStreamLoader2(String filename, int numberOfStreams, double ratio) {
		this.numberOfStreams=numberOfStreams;
		this.filename=filename;
		openFile(filename);
		this.ratio=ratio;
		this.throwAwayLines=(int)(1d/this.ratio)-1;
	}
	
	public ProcessedStreamLoader2(String filename, int numberOfStreams, double ratio, int repeat) {
		this.numberOfStreams=numberOfStreams;
		this.filename=filename;
		openFile(filename);
		this.ratio=ratio;
		this.throwAwayLines=(int)(1d/this.ratio)-1;
		rn = new Random(repeat);
	}
	
	public static boolean fileExists(String fname) {
		File f = new File(fname);
		return f.exists();
	}

}
