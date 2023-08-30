package omniVLDB.structure;

public class logEventInt {
	public final int ipaddress; //client's ip
	public int seconds;
	public final int file; // request url
	public final int streamid;
	public int getEvent() {
		return file;
	}
	public int getTime() {
		return seconds;
	}
	public logEventInt(int ipaddress, int seconds, int file, int streamid) {
		this.seconds=seconds;
		this.ipaddress=ipaddress;
		this.streamid=streamid;
		this.file=file;
	}
	public logEventInt(logEvent levent) {
		this.seconds=levent.seconds;
		this.ipaddress=levent.ipaddress;
		this.streamid=levent.streamid;
		this.file=levent.file.hashCode();
	}
	public String toString() {
		return "IP:" + ipaddress + " Time:" + seconds + " SID:" + streamid+ " FileID:" + file;
	}
}