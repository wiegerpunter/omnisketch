package omniVLDB.dataGeneration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import omniVLDB.structure.logEvent;



public class StreamLoader {
	long lastTime=-1;
	long starttime=-1;
	static String pattern = "wc_day";
	String path;
	int dayno=1,fileno=1;
	boolean fileExists=true;
	String currentFilename;
	BufferedReader br;

	public int getStartTime() {
		int dayno=1, fileno=1;
		String filename = getFilename(path, dayno, fileno);
		if (fileExists(filename)) {
			openFile(path, dayno, fileno);
			logEvent lev = processLine(readNextLine());
			return lev.seconds;
		}
		else return -1;
	}
	
	public int getStopTime() {
		int dayno=1, fileno=1;
		boolean fileExists=fileExists(getFilename(path, dayno, fileno));
		while (fileExists) {
			dayno++;
			System.err.println("Checking file " + getFilename(path, dayno, fileno));
			fileExists=fileExists(getFilename(path, dayno, fileno));
		}
		System.err.println("File " + getFilename(path, dayno, fileno)+ " does not exist");
		dayno--;
		
		fileExists=fileExists(getFilename(path, dayno, fileno));
		while (fileExists) {
			fileno++;
			System.err.println("Checking file " + getFilename(path, dayno, fileno));
			fileExists=fileExists(getFilename(path, dayno, fileno));
		}
		System.err.println("File " + getFilename(path, dayno, fileno)+ " does not exist");
		fileno--;

		openFile(path, dayno, fileno);
		logEvent lev = processLine(readLastLineWithoutProcessing());
		return lev.seconds;
	}
	
	public void reset() {
		this.dayno=1;
		this.fileno=1;
		openFile(path, dayno, fileno);
	}
	
	public void openFile(String path, int dayno, int fileno) {
		try {
			if (br!=null) br.close();
		} catch (Exception ignored){}
		try {
			System.err.println("Opening file " + getFilename(path, dayno, fileno));
			GZIPInputStream gzipinput = new GZIPInputStream(new FileInputStream(getFilename(path, dayno, fileno)));
			br = new BufferedReader(new InputStreamReader(gzipinput));
		} catch (Exception e){e.printStackTrace();}
	}
	int failedToParse=0;
	
	HashMap<Integer, Integer> streamids=new HashMap<Integer, Integer>();

	static Random rn = new Random(123);
	
	public logEvent processLineOld(final String s) {
		try {
			int firstdash = s.indexOf('-');
			int seconddash = s.indexOf('-', firstdash+1);
			int firstGet = s.indexOf('"', seconddash+1);
			int filenameENDS = s.indexOf(" HTTP/", firstGet+1);
			int lastDoubleThingy= s.lastIndexOf('"');
			int lastSpace = s.lastIndexOf(' ');
			int prelastSpace = s.lastIndexOf(' ',lastSpace-1);

			int ipaddress = Integer.parseInt(s.substring(0, firstdash-1));
			String datestr = s.substring(seconddash+3, firstGet-8);
			SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss"); //please notice the capital M
			Date d=null;
			try {
				d = formatter.parse(datestr);
			}catch (Exception e){e.printStackTrace();};
			String filename = s.substring(firstGet+5, filenameENDS).intern();
			short result = Short.parseShort(s.substring(lastDoubleThingy+2, prelastSpace));
			int serverid = Integer.parseInt(s.substring(prelastSpace+1, lastSpace));
			int size=0;
			String sizeStr = s.substring(lastSpace+1);
			if (result!=304 && sizeStr.length()>1)
				size = Integer.parseInt(sizeStr);
			long thisTime=d.getTime();
			if (starttime==-1) starttime=thisTime;
//			if (lastTime>thisTime) 
//				System.err.println("This is out of order");
			lastTime=thisTime;
			logEvent levent =null;
			if (numberOfStreams>0)
				levent = new logEvent(ipaddress, (int)((thisTime-starttime)/1000l), filename, result, size, (int)(thisTime/1000%numberOfStreams)); // to start from zero, and make debugging easier
			else {
				// convert serverid from 0 to maxservers
				Integer serveridinc = streamids.get(serverid);
				if (serveridinc!=null)
					serverid=serveridinc;
				else {
					streamids.put(serverid, streamids.size());
					serverid=streamids.size()-1;
				}
				levent = new logEvent(ipaddress, (int)((thisTime-starttime)/1000l), filename, result, size, serverid); // to start from zero, and make debugging easier
			}
			return levent;
		} catch (Exception e){
			failedToParse++;
		}
		return null;
	}

	final SimpleDateFormat formatter;
	final short tmp=0;

	public logEvent processLine(final String s) {
		if (numberOfStreams < 0) {
			char[] allchars = s.toCharArray();
			int firstdash = 0;
			while (allchars[firstdash] != '-')  firstdash++;
			int seconddash = firstdash + 1;
			while (allchars[seconddash] != '-') seconddash++;
			int firstGet = seconddash + 32;
			int filenameENDS = firstGet + 5;
			while (allchars[filenameENDS] != ' ') filenameENDS++;
			int lastDoubleThingy = filenameENDS + 7;
			while (allchars[lastDoubleThingy] != '"') lastDoubleThingy++;
			int prelastSpace = lastDoubleThingy + 2;
			while (allchars[prelastSpace] != ' ') prelastSpace++;
			int lastSpace = prelastSpace + 1;
			while (allchars[lastSpace] != ' ') lastSpace++;

			try {
				int ipaddress = Integer.parseInt(s.substring(0, firstdash - 1));
				String datestr = s.substring(seconddash + 3, firstGet - 9);
				Date d = null;
				try {
					d = formatter.parse(datestr);
				} catch (Exception e) {
					e.printStackTrace();
				}
				;
				String filename = s.substring(firstGet + 4, filenameENDS).intern();
				short result = Short.parseShort(s.substring(lastDoubleThingy + 2, prelastSpace));
				int serverid = Integer.parseInt(s.substring(prelastSpace + 1,lastSpace));
				int size = 0;
				String sizeStr = s.substring(lastSpace + 1);
				if (result != 304 && sizeStr.length() > 1) size = Integer.parseInt(sizeStr);
				long thisTime = d.getTime();
				if (starttime == -1) starttime = thisTime;
				logEvent levent = null;
				if (numberOfStreams > 0)
					levent = new logEvent(ipaddress, (int) ((thisTime - starttime) / 1000l), filename,
							result, size, rn.nextInt(numberOfStreams)); // to start from zero, and make debugging easier
				else {
					// convert serverid from 0 to maxservers
					Integer serveridinc = streamids.get(serverid);
					if (serveridinc != null)
						serverid = serveridinc;
					else {
						streamids.put(serverid, streamids.size());
						serverid = streamids.size() - 1;
					}
					levent = new logEvent(ipaddress, (int) ((thisTime - starttime) / 1000l), filename, result, size, serverid); // to start from zero, and make debugging easier
				}
				return levent;
			} catch (Exception e) {
				failedToParse++;
			}
			return null;
		} else {
            int firstBracket = s.indexOf('[');
            int rightIsagogika=s.lastIndexOf('"', s.length()-10);
            if(firstBracket+34>rightIsagogika-9)
                rightIsagogika=s.lastIndexOf('"');

            String datestr = s.substring(firstBracket+1, firstBracket+21);
            String filename = s.substring(firstBracket+34, rightIsagogika-9).intern();
            Date d=null;
			try {
				d = formatter.parse(datestr);
			}catch (Exception e){e.printStackTrace(); return null;};
			long thisTime=d.getTime();
			if (starttime==-1) starttime=thisTime;
			try {
				short result=Short.parseShort(s.substring(rightIsagogika+2,s.indexOf(' ', rightIsagogika+3)));
				logEvent levent = new logEvent(0, (int)((thisTime-starttime)/1000l), filename,result, 0, (int)(thisTime/1000%numberOfStreams)); // to start from zero, and make debugging easier
				return levent;
			} catch (Exception e) {
				failedToParse++;
			}
			return null;
		}
	}
	public int getFailedToParse() {
		return failedToParse;
	}
	
	public String readNextLine() {
		try {
			if (!br.ready()) { // read next file
				fileno++;
				if (!fileExists(getFilename(path, dayno, fileno)))  {
					fileno=1;
					dayno++;
					if (!fileExists(getFilename(path, dayno, fileno))) 
						return null;
					else {
						openFile(path, dayno, fileno);
					}
				} else {
					openFile(path, dayno, fileno);
				}
			}
			String line = br.readLine();
			if (line!=null) // not an empty line
				return line;
			else 
				return readNextLine(); // recursion will happen max 1 time
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public String readLastLineWithoutProcessing() {
		try {
			String line2=null;
			while (br.ready()) {
				String line = br.readLine();
				if (line.length()<2) 
					return line2;
				else 
					line2=line;
			}
			return line2; // this will never be invoked
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	final int numberOfStreams;
	public StreamLoader(String path, int numberOfStreams) {
		Locale.setDefault(Locale.ENGLISH);
		formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss"); // notice the capital M
		this.numberOfStreams=numberOfStreams;
		this.path=path;
		openFile(path, dayno, fileno);
	}
	
	public static void main(String[] args) {
		int numberOfStreams=-1;
		String path=args[0];
		StreamLoader sl = new StreamLoader(path,numberOfStreams);
		String s;
		while (true){
			s=sl.readNextLine();
			if (s==null) break;
			else {
				logEvent event = sl.processLine(s);
				System.err.println(s);
			}
		}
		System.err.println("Failed to parse:" + sl.failedToParse);
	}

	public static String getFilename(String path, int dayno, int fileno) {
		String fname = path+File.separator + pattern + dayno+"_"+fileno + ".gz";
		return fname;
	}
	
	public static boolean fileExists(String fname) {
		File f = new File(fname);
		return f.exists();
	}

}
