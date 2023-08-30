package omniVLDB.structure;

import java.util.ArrayList;
import java.util.Collections;

public class StreamHD {
	int currenttime;
	final EventHD[] events;
	public EventHD[] getEvents() {
		return events;
	}
	public StreamHD(EventHD[] events) {
		this.events=events;
		this.currenttime=this.events[this.events.length-1].time;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (EventHD e:events) {
			sb.append(e.toString() + " ");
		}
		return sb.toString();
	}
	public int getCurrentTime() {
		return currenttime;
	}
	
	public static StreamHD mergeStreamsKeepOnly(StreamHD[] allStreams, int timeFrom) {
		int totalEvents = 0;
		for (StreamHD s:allStreams) totalEvents+=s.getEvents().length;
				
		ArrayList<EventHD> allEvents = new ArrayList<EventHD>(totalEvents);
		for (StreamHD s:allStreams) {
			for (EventHD e:s.getEvents()) {
				if (e.time>=timeFrom) allEvents.add(e);
			}
		}
		Collections.sort(allEvents);
		EventHD[] evs = new EventHD[allEvents.size()];
		evs = allEvents.toArray(evs);
		StreamHD s = new StreamHD(evs);
		return s;
	}

	public static StreamHD mergeStreams(StreamHD[] allStreams) {
		int totalEvents = 0;
		for (StreamHD s:allStreams) totalEvents+=s.getEvents().length;
				
		ArrayList<EventHD> allEvents = new ArrayList<EventHD>(totalEvents);
		for (StreamHD s:allStreams) 
			for (EventHD e:s.getEvents()) {
				allEvents.add(e);
			}
		Collections.sort(allEvents);
		EventHD[] evs = new EventHD[allEvents.size()];
		evs = allEvents.toArray(evs);
		StreamHD s = new StreamHD(evs);
		return s;
	}
}