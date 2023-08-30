package omniVLDB.dataGeneration;

import java.util.Random;

import omniVLDB.structure.EventHD;
import omniVLDB.structure.logEventInt;

public class artificialStream {
	EventHD[] allEvents;
	int numberOfNodes;
	Random rn = null;
	public artificialStream(EventHD[] allEvents, int numberOfNodes, int seed) {
		this.allEvents = allEvents;
		this.numberOfNodes=numberOfNodes;
		rn = new Random(seed);
	}
	int cnt=0;
	public logEventInt getNext() {
		if (cnt==allEvents.length)
			return null;
		EventHD ev = allEvents[cnt++];
		logEventInt levent = new logEventInt(0, ev.time, ev.event, rn.nextInt(this.numberOfNodes));
		return levent;
	}
}

