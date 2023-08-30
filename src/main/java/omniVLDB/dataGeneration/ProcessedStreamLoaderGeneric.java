package omniVLDB.dataGeneration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import omniVLDB.*;
import omniVLDB.Record;
import omniVLDB.structure.logEventInt;

public class ProcessedStreamLoaderGeneric {
	static boolean compression=true;

	public int skip=0;
	public int linesSeen = 0;
	public void setFilename(String filename) {
		this.filename = filename;
	}
	DataInputStream dis;
	BufferedReader bis;
	int start,stop;
	public int getStart(){
		return start;
	}
	public int getStop() {
		return stop;
	}
	int recSkippedBecauseEarlyStart=0;
	String filename = null;
	public void reset() {
		try {
			if (compression) {
				BufferedInputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename)));
				//dis = new DataInputStream(in);
				bis = new BufferedReader(new InputStreamReader(in));
				for (int i = 0; i < 4; i++) {
					bis.readLine();
				}
			} else {
				BufferedInputStream in = new BufferedInputStream(new FileInputStream(filename));
				//dis = new DataInputStream((new BufferedInputStream(new FileInputStream(filename))));
				bis = new BufferedReader(new InputStreamReader(in));
			}
			//start = dis.readInt();
			//stop = dis.readInt();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public ProcessedStreamLoaderGeneric(String filename, boolean compression) {
		//int bufferSize = 32768*256*16;//1024*1024;
//		System.err.println("Buffer size is " + bufferSize);
		this.filename = filename;
		this.compression = compression;
		try {
			if (compression) {
				BufferedInputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(filename)));
				//dis = new DataInputStream(in);
				bis = new BufferedReader(new InputStreamReader(in));

			} else {
				BufferedInputStream in = new BufferedInputStream(new FileInputStream(filename));
				//dis = new DataInputStream((new BufferedInputStream(new FileInputStream(filename))));
				bis = new BufferedReader(new InputStreamReader(in));
			}
			//start = dis.readInt();
			//stop = dis.readInt();
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	public String readFirst() {
		try {
			String b = bis.readLine();
			linesSeen++;
			if (filename.contains("snmp")) {
				while ('#' == b.charAt(0)) {
					b = bis.readLine();
					linesSeen++;
				}
			} else {
				while ('f' == b.charAt(0)) {
					//System.err.println("Skipping header");
					b = bis.readLine();
					linesSeen++;
				}
			}
			return b;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public String readRecord(String line, int id, ArrayList<Record> d) {
		//Record r = null;
		Record r = null;
		if (omniVLDB.Main.datasetName.equals("SNMP"))
			r = new RecordSNMP(r);
		else if (omniVLDB.Main.datasetName.equals("CAIDA"))
			r = new RecordCAIDA(r);
		else {
			throw new RuntimeException("Unknown dataset name");
		}
		//Record r = new Record(); // record to be potentially added to d.
		r.setId(id);

		try {
			String b = line;
			if (b == null)
				throw new Exception("Null line");
			if ('#' == b.charAt(0)) {
				throw new Exception("Somehow got a comment line");
			}
			if (Main.datasetName.equals("SNMP") && !('s' == b.charAt(0))) {
				throw new Exception("Somehow got a non-start line");
			}

			String[] a = b.split(",");
			r.add(a); // Add the start line to the record.

			b = bis.readLine(); // Read second line.
			linesSeen++;

			if (Main.datasetName.equals("SNMP")) {
				// // Dont read attributes 3, 16, 17, 18, 19, 20.
				a = b.split(",");
				int cnt = 0;
				while (true) {
					if ('s' == a[0].charAt(0)) {
						if (r.assembled) {
							Record r2 = (Record) r;
							r = null;
							d.add(r2);
//							if (r2.complete) {
//								d.add(r2);
//							} else {
//								skip++;
//								//throw new Exception("Record not assembled.");
//							}
						} else {
							skip++;
							//throw new Exception("Record not assembled.");
						}
						return b;
					} else if ('i' == a[0].charAt(0)) {
						//dont read attributes 3, 16, 17, 18, 19, 20.

						r.add(a);
						cnt++; // 0 at start, 1 at second, 2 at third, 3 at fourth.
					}
					b = bis.readLine();
					linesSeen++;
					if (b == null) { // End of file.
						skip++;
						return null;
					}
					a = b.split(",");
				}
			} else {
				if (b == null) { // End of file.
					skip++;
					return null;
				}
				r.assemble(id);
				Record r2 = null;
				if (Main.datasetName.equals("SNMP")) {


					r2 = new Record( (RecordSNMP) r) {
						@Override
						public void assemble(int id) {
							assembled = true;
						}
					};
				} else {
					r2 = new Record((RecordCAIDA) r) {
						@Override
						public void assemble(int id) {
							assembled = true;
						}
					};
				}
				r = null;
				d.add(r2);
				return b;
			}
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	
	public logEventInt readNextEvent() {
		try {
			int ipaddress = dis.readInt();
			int seconds = dis.readInt();
			int file = dis.readInt();
			int streamid = dis.readInt();
			return new logEventInt(ipaddress, seconds, file, streamid);
		} catch (Exception e) {
		}
		return null;
	}

	public logEventInt readNextEvent(int numberOfStreams) {
		try {
			int ipaddress = dis.readInt();
			int seconds = dis.readInt();
			int file = dis.readInt();
			int streamid = dis.readInt();
			if (numberOfStreams>0)
				streamid = ipaddress%numberOfStreams;
			return new logEventInt(ipaddress, seconds, file, streamid);
		} catch (Exception e) {
		}
		return null;
	}

	public logEventInt readDump(int timeUntil, int numberOfStreams) {
		byte[] bb = new byte[1024*1024];
		try {
		while (true) {
			dis.readFully(bb);
			logEventInt levent = this.readNextEvent(numberOfStreams);
			if (levent.seconds>timeUntil) return levent;
		}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void readFile () {
		logEventInt l = readNextEvent();
		long checksum=0;
		HashSet<Integer> streams=new HashSet<>();
		while (l!=null) {
			checksum+=(l.ipaddress+l.seconds+l.file+l.streamid)%10;
			streams.add(l.streamid);
			l = readNextEvent();
		}
		System.err.println("Read checksum is " + checksum % 100000);
		System.err.println("Number of streams is " + streams.size());
	}
	
	public static void mainReadOnly(String[]args) {
		ProcessedStreamLoaderGeneric psl  = new ProcessedStreamLoaderGeneric(args[0], compression);
		psl.readFile();
	}
	
	public static void main(String[] args) throws Exception {
		if (args[args.length-1].equals("compress"))
			compression=true;
		System.err.println("Compress is " + compression);
		long startTime= System.currentTimeMillis();
		convert(args[0]);
		long stopTime= System.currentTimeMillis();
		System.err.println("Write took "+ (stopTime-startTime)/1000);
		
		String outfile=null;
		if (compression) {
			if (args[0].contains("wc-part")) {
				outfile = "wc-part.binary.gz";
			} else if (args[0].contains("wc-")) {
				outfile = "wc-all.binary.gz";
			} else {
				outfile = "snmp.binary.gz";
			}
		} else {
			if (args[0].contains("wc-part")) {
				outfile = "wc-part.binary";
			} else if (args[0].contains("wc-")) {
				outfile = "wc-all.binary";
			} else {
				outfile = "snmp.binary";
			}
		}
		ProcessedStreamLoaderGeneric psl  = new ProcessedStreamLoaderGeneric(outfile, compression);
		psl.readFile();
		long newstopTime= System.currentTimeMillis();
		System.err.println("Read took "+ (newstopTime-stopTime)/1000);

	}

	public void readHeader() {

	}

	public void close() {
		try {
			bis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	public static void convert(String path) throws Exception {
		long checksum=0;
		int start,stop = 0;
		String outfile;
		DataOutputStream dos = null;
		if (path.endsWith("sc.txt.gz")) {
			start=0;
			stop=10367966;
			if (compression)
				outfile = "snmp.binary.gz";
			else
				outfile = "snmp.binary";
			// now read and write
			ProcessedStreamLoader2 psl = new ProcessedStreamLoader2(path, -1, 1);
			if (compression)
				dos = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(outfile))));
			else
				dos = new DataOutputStream((new BufferedOutputStream(new FileOutputStream(outfile))));

			dos.writeInt(start); dos.writeInt(stop);
			while (true) {
				logEventInt levent = psl.readNextLineInt2();
				if (levent==null)
					break;
				dos.writeInt(levent.ipaddress);dos.writeInt(levent.seconds);dos.writeInt(levent.file); dos.writeInt(levent.streamid);
			}
			dos.flush();
			dos.close();
		} else if (path.contains("wc-")){ // wc-all.gz and wc-part.gz
			if (path.contains("wc-part")) {
				start=0;
				stop=768131;
				if (compression)
					outfile = "wc-part.binary.gz";
				else
					outfile = "wc-part.binary";
			} else {
				start=0;
				stop=7518578;
				if (compression)
					outfile = "wc-all.binary.gz";
				else
					outfile = "wc-all.binary";
			}
			ProcessedStreamLoader psl = new ProcessedStreamLoader(path, -1, 1);

			if (compression)
				dos = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(outfile))));
			else
				dos = new DataOutputStream((new BufferedOutputStream(new FileOutputStream(outfile))));

			dos.writeInt(start); dos.writeInt(stop);
			while (true) {
				logEventInt levent = psl.readNextLineInt();
				if (levent==null)
					break;
				dos.writeInt(levent.ipaddress);dos.writeInt(levent.seconds);dos.writeInt(levent.file); dos.writeInt(levent.streamid);
				checksum+=(levent.ipaddress+levent.seconds+levent.file+levent.streamid)%10;
			}
			dos.flush();
			dos.close();
		}
		System.err.println("Write checksum is " + checksum % 100000);
	}



}
