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
 
public class ProcessedStreamLoader {
	long lastTime=-1;
	long starttime=-1;
	String filename="sc.txt.gz";
	boolean fileExists=true;
	String currentFilename;
	BufferedReader br;
	long line=0;
	final double ratio;
	final int throwAwayLines;
	public void reset() {
		openFile(filename);
	}
	public void reset2() {
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
	
	HashMap<Integer, Integer> streamids=new HashMap<Integer, Integer>();

	static Random rn = new Random();
	
	final short tmp=0;
	public logEventInt readNextLineInt2() {
		try {
			if (!br.ready())
				return null;
			else {
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready()) 
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = Integer.parseInt(br.readLine());
				int streamid = Integer.parseInt(br.readLine());
				int filename = Integer.parseInt(br.readLine());
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				if (this.numberOfStreams>0)
					return new logEventInt(ipaddress, time, filename, (int)((time/1000)%numberOfStreams));
				else
					return new logEventInt(ipaddress, time, filename, streamid); // streamid in files starts from 0
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}

	Random rn2 = new Random(123);
	public logEventInt readNextLineInt3() {
		try {
			if (!br.ready())
				return null;
			else {
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready()) 
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = Integer.parseInt(br.readLine());
				int streamid = Integer.parseInt(br.readLine());
				int filename = Integer.parseInt(br.readLine());
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				if (this.numberOfStreams>0)
					return new logEventInt(ipaddress, time, filename, rn2.nextInt(numberOfStreams));
				else
					return new logEventInt(ipaddress, time, filename, streamid); // streamid in files starts from 0
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}
	
	
	public logEventInt readNextLineIntOnlyTime() {
		try {
			if (!br.ready())
				return null;
			else {
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready()) 
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = 0;br.readLine();
				int streamid = Integer.parseInt(br.readLine());
				int filename = 0;br.readLine();
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				if (this.numberOfStreams>0)
					return new logEventInt(ipaddress, time, filename, (int)((time+rn.nextInt(numberOfStreams))%numberOfStreams)   );
				else
					return new logEventInt(ipaddress, time, filename, streamid); // streamid in files starts from 0
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}
	public logEventInt readNextLineIntOnlyTimeFile() {
		try {
			if (!br.ready())
				return null;
			else {
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready()) 
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = 0;br.readLine();
				int streamid = 0;br.readLine();
				int filename = Integer.parseInt(br.readLine());
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				return new logEventInt(ipaddress, time, filename, streamid);
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}
	
	public logEventInt readNextLineInt() {
		try {
				{
				if (throwAwayLines>0) {
					int cnt=0;
					while (cnt!=throwAwayLines && br.ready()) { // skip cntRound files
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						br.readLine(); // comes in batch of four always
						cnt++;
					}
				}
				if (!br.ready())
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = Integer.parseInt(br.readLine());
				int streamid = Integer.parseInt(br.readLine());
				int filename = Integer.parseInt(br.readLine())+1; // to avoid file with id=0
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				if (this.numberOfStreams>0)
					return new logEventInt(ipaddress, time, filename, (int)(time/1000%numberOfStreams));
				else
					return new logEventInt(ipaddress, time, filename, streamid); // streamid in files starts from 0
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}
	public final logEventInt readNextLineIntSingleStream() {
		try {
				{
				if (!br.ready())
					return null;
				line++;
				int time = Integer.parseInt(br.readLine());
				int ipaddress = Integer.parseInt(br.readLine());
				int streamid = 0; br.readLine();//Integer.parseInt(br.readLine());
				int filename = Integer.parseInt(br.readLine());
				if (line%10000000==0) 
					System.err.println("Processed lines (millions):" + line/1000000);
				return new logEventInt(ipaddress, time, filename, streamid);
			}
		} catch (Exception e) {
			e.printStackTrace(); return null;
		}
	}

	final int numberOfStreams;
	public ProcessedStreamLoader(String filename, int numberOfStreams, double ratio) {
		this.numberOfStreams=numberOfStreams;
		this.filename=filename;
		openFile(filename);
		this.ratio=ratio;
		this.throwAwayLines=(int)(1d/this.ratio)-1;
	}
	
	public ProcessedStreamLoader(String filename, int numberOfStreams, double ratio, int repeat) {
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
