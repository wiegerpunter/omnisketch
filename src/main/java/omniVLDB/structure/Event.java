package omniVLDB.structure;

public class Event implements Comparable<Event>  {
	boolean event;
	int time;

	public int getTime() {
		return time;
	}
	public boolean getEvent() {
		return event;
	}
	public boolean comesAtOrAfter(int t) {
		return (time>=t);
	}
	public Event(boolean event, int time) {
		this.event=event;
		this.time=time;
	}
	public String toString(){
		return "("+event + ","+time+")";
	}

	public int compareTo(Event e1) {
		if (this.time>e1.time) return 1;
		else if (this.time<e1.time) return -1;
		else return 0;
	}
}

