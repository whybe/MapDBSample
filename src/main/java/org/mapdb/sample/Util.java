package org.mapdb.sample;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public class Util {
	
	public static byte[] getRandomByteArray(int length) {
		return getRandomByteArray(length, length);
	}

	public static byte[] getRandomByteArray(int start, int end) {
		return RandomUtils.nextBytes(RandomUtils.nextInt(start, end));
	}
	
	public static String getRandomString(int length) {
		return getRandomString(length, length);
	}

	public static String getRandomString(int start, int end) {
		return RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(start, end));
	}
}
