package omniVLDB.structure;

import java.util.ArrayList;
import java.util.Collections;

public class Stream {
	int currenttime;
	final Event[] events;
	public Event[] getEvents() {
		return events;
	}
	public Stream(Event[] events) {
		this.events=events;
		this.currenttime=this.events[this.events.length-1].time;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Event e:events) {
			sb.append(e.toString() + " ");
		}
		return sb.toString();
	}
	public int getCurrentTime() {
		return currenttime;
	}
	
	public static Stream mergeStreamsKeepOnly(Stream[] allStreams, int timeFrom) {
		int totalEvents = 0;
		for (Stream s:allStreams) totalEvents+=s.getEvents().length;
				
		ArrayList<Event> allEvents = new ArrayList<Event>(totalEvents);
		for (Stream s:allStreams) {
			for (Event e:s.getEvents()) {
				if (e.time>=timeFrom) allEvents.add(e);
			}
		}
		Collections.sort(allEvents);
		Event[] evs = new Event[allEvents.size()];
		evs = allEvents.toArray(evs);
		Stream s = new Stream(evs);
		return s;
	}

	public static Stream mergeStreams(Stream[] allStreams) {
		int totalEvents = 0;
		for (Stream s:allStreams) totalEvents+=s.getEvents().length;
				
		ArrayList<Event> allEvents = new ArrayList<Event>(totalEvents);
		for (Stream s:allStreams) 
			for (Event e:s.getEvents()) {
				allEvents.add(e);
			}
		Collections.sort(allEvents);
		Event[] evs = new Event[allEvents.size()];
		evs = allEvents.toArray(evs);
		Stream s = new Stream(evs);
		return s;
	}
}




