package org.mapdb.sample;

public class StopWatch {
	
	private static long st;

	public static void start() {
		st = Timer.getCurrentTime();
	}

	public static void stop(String string) {
		log(string + " : " + Timer.getElapsedTime(st) + "ms");
	}
	
	public static void reset(String string) {
		stop(string);
		start();
	}
	
	private static void log(String string) {
		System.out.println(string);
	}

}
