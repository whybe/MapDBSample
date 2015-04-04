package org.mapdb.sample;

public class Timer {
	public static long getCurrentTime() {
		return System.currentTimeMillis();
	}

	public static String getElapsedTime(long st) {
		return String.valueOf(System.currentTimeMillis() - st);
	}
}
