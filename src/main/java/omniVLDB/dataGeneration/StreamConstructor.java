package omniVLDB.dataGeneration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import cern.jet.random.Normal;
import cern.jet.random.Poisson;
import cern.jet.random.Uniform;
import cern.jet.random.engine.MersenneTwister;

import omniVLDB.structure.EventHD;
import omniVLDB.structure.Stream;
import omniVLDB.structure.Event;
import omniVLDB.structure.StreamHD;

public class StreamConstructor {
	final Uniform uniformDistribution;
	cern.jet.random.AbstractDistribution distribution;
	final cern.jet.random.engine.MersenneTwister rn;

	public StreamConstructor(int init, int timeRange) {
		rn = new MersenneTwister(init);
		uniformDistribution=new Uniform(0,timeRange,rn); 
	}
	public StreamConstructor(int init) {
		rn = new MersenneTwister(init);
		uniformDistribution=new Uniform(0,1000,rn); 
	}
	
	public StreamHD constructHighDimensionalStreamExponential(Stream s, int EVENT_TYPES) {
		MersenneTwister mt = new MersenneTwister();

		
		EventHD[] events = new EventHD[s.getEvents().length];
		for (int cnt=0;cnt<events.length;cnt++) {
			Event e = s.getEvents()[cnt];
			int eventType = (int)cern.jet.random.Distributions.nextPowLaw(1.1, EVENT_TYPES, mt);
			EventHD ee = new EventHD(eventType, e.getTime());
			events[cnt]=ee;
		}
		int[] freqs = new int[EVENT_TYPES];
		for (int i=0;i<events.length;i++) freqs[events[i].getEvent()]++;
		for (int i=0;i<EVENT_TYPES;i++) System.err.println(i + " " + freqs[i]);
		
		return new StreamHD(events);
		
	}

	public StreamHD constructHighDimensionalStreamZipfian(Stream s, int EVENT_TYPES, double exponent) {
		MersenneTwister mt = new MersenneTwister();

		
		EventHD[] events = new EventHD[s.getEvents().length];
		for (int cnt=0;cnt<events.length;cnt++) {
			Event e = s.getEvents()[cnt];
			int eventType = (int)cern.jet.random.Distributions.nextZipfInt(4, mt)-1;
			while (eventType>=EVENT_TYPES) eventType = (int)cern.jet.random.Distributions.nextZipfInt(exponent, mt)-1;
			EventHD ee = new EventHD(eventType, e.getTime());
			events[cnt]=ee;
		}
//		int[] freqs = new int[EVENT_TYPES];
//		for (int i=0;i<events.length;i++) freqs[events[i].getEvent()]++;
//		for (int i=0;i<EVENT_TYPES;i++) System.err.println(freqs[i]);
		
		return new StreamHD(events);
		
	}

	public StreamHD constructHighDimensionalStreamPoisson(Stream s, int EVENT_TYPES) {
		MersenneTwister mt = new MersenneTwister();
		Poisson p = new Poisson(Math.abs(rn.nextInt()%EVENT_TYPES), mt);
		EventHD[] events = new EventHD[s.getEvents().length];
		for (int cnt=0;cnt<events.length;cnt++) {
			Event e = s.getEvents()[cnt];
			int eventType = Math.abs(p.nextInt()%EVENT_TYPES);
			EventHD ee = new EventHD(eventType, e.getTime());
			events[cnt]=ee;
		}
		return new StreamHD(events);
	}

	public StreamHD constructHighDimensionalZipfStream(Stream s, int EVENT_TYPES, double alpha) {
		System.err.println(s.getEvents().length);
		double[] vals = new double[EVENT_TYPES];
		double totalVal = 0;
		for (int i=0;i<EVENT_TYPES;i++) {
			vals[i] = 1d/Math.pow(i+1,alpha);
			totalVal+=vals[i];
		}
		int numberOfEvents = s.getEvents().length;
		ArrayList<Integer> evIds = new ArrayList<>(s.getEvents().length+EVENT_TYPES);
		for (int i=0;i<EVENT_TYPES;i++) {
			vals[i]/=totalVal;
			for (int j=0;j<(vals[i]*numberOfEvents);j++) evIds.add(i);
		}
		Collections.shuffle(evIds);	Collections.shuffle(evIds);

		EventHD[] events = new EventHD[numberOfEvents];
		Iterator<Integer> it = evIds.iterator();
		for (int cnt=0;cnt<numberOfEvents;cnt++) {
			Event e = s.getEvents()[cnt];
			int eventType = it.next();
			EventHD ee = new EventHD(eventType, e.getTime());
			events[cnt]=ee;
		}
		return new StreamHD(events);
	}


	public StreamHD constructHighDimensionalStream(Stream s, int EVENT_TYPES) {
		EventHD[] events = new EventHD[s.getEvents().length];
		Event[] ev1d  = s.getEvents();
		for (int cnt=0;cnt<events.length;cnt++) {
			Event e = ev1d[cnt];
			int eventType = Math.abs(rn.nextInt()%EVENT_TYPES);
			EventHD ee = new EventHD(eventType, e.getTime());
			events[cnt]=ee;
		}
		return new StreamHD(events);
	}
	// constructs a stream with numberOfEvents TRUE events
	// false events do not have any role, so they are not even generated
	public Stream constructUniformStream(int numberOfEvents) {
		Event[] allEvents = new Event[numberOfEvents];
		ArrayList<Event> tmpArray = new ArrayList<Event>(numberOfEvents);
		for (int cnt=0;cnt<numberOfEvents;cnt++) {
			int val = uniformDistribution.nextInt();
			tmpArray.add(new Event(true,val));	
		}
		Collections.sort(tmpArray);
		allEvents=tmpArray.toArray(allEvents);
		Stream s = new Stream(allEvents);
		return s;
	}

	// constructs a stream with numberOfEvents TRUE events, following the Poisson distribution
	// false events do not have any role, so they are not even generated
	public Stream constructPoissonStream(int numberOfEvents, double mean) {
		distribution = new Poisson(mean, rn);
		Event[] allEvents = new Event[numberOfEvents];
		ArrayList<Event> tmpArray = new ArrayList<Event>(numberOfEvents);
		for (int cnt=0;cnt<numberOfEvents;cnt++) {
			int val = distribution.nextInt()+uniformDistribution.nextInt();
			tmpArray.add(new Event(true,val));
		}
		Collections.sort(tmpArray);
		allEvents=tmpArray.toArray(allEvents);
		Stream s = new Stream(allEvents);
		return s;
	}
	

	// constructs a stream with numberOfEvents TRUE events, following the Poisson distribution
	// false events do not have any role, so they are not even generated
	public Stream constructNormalStream(int numberOfEvents, double mean, double stdev) {
		distribution = new Normal(mean, stdev, rn);
		Event[] allEvents = new Event[numberOfEvents];
		ArrayList<Event> tmpArray = new ArrayList<Event>(numberOfEvents);
		for (int cnt=0;cnt<numberOfEvents;cnt++) {
			int val = distribution.nextInt();
			tmpArray.add(new Event(true,val));
		}
		Collections.sort(tmpArray);
		allEvents=tmpArray.toArray(allEvents);
		Stream s = new Stream(allEvents);
		return s;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		StreamConstructor sc = new StreamConstructor(1234,1000);
		Stream s = sc.constructUniformStream(10);
		System.err.println("Uniform " + s);

		Stream s2 = sc.constructPoissonStream(10, 5);
		System.err.println("Poisson " + s2);

		Stream s3 = sc.constructNormalStream(10, 5, 2);
		System.err.println("Normal " + s3);
	}

}
