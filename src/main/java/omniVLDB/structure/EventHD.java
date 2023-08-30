package omniVLDB.structure;

public class EventHD implements Comparable<EventHD>{
    public int event;
    public int time;

    public int getTime() {
        return time;
    }
    public int getEvent() {
        return event;
    }
    public boolean comesAtOrAfter(int t) {
        return time>=t;
    }
    public EventHD(int event, int time) {
        this.event=event;
        this.time=time;
    }
    public String toString(){
        return "("+event + ","+time+")";
    }

    public int compareTo(EventHD e1) {
        if (this.time>e1.time) return 1;
        else if (this.time<e1.time) return -1;
        else return 0;
    }
}