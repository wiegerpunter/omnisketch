package omniVLDB.structure;

public class logEvent {
	public int ipaddress;
	public int seconds;
	public String file;
	public short result;
	public int size;
	public int streamid;
	public logEvent(int ipaddress, int seconds, String file, short result, int size, int streamid) {
		this.ipaddress=ipaddress;
		this.seconds=seconds;
		this.file=file;
		this.result=result;
		this.size=size;
		this.streamid=streamid;
	}
}