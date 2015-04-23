package org.mapdb.sample;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;

import org.apache.commons.io.FileUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeKeySerializer.ArrayKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;

public class MultiMapSample {

	public static final String MULTIMAP_NAME_STRING_AND_INTEGER = "multimapsi";
	public static final String MULTIMAP_NAME_STRING_AND_BYTE_ARRAY = "multimapsba";
	public static final String RESOURCE_PATH = "src/main/resources/org/mapdb/sample";
	public static final String DB_FILE = "multimap.mapdb";

	public static void main(String[] args) throws IOException {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
		if (dbFile.exists()) {
			FileUtils.forceDelete(dbFile);
		} else {
			FileUtils.touch(dbFile);
		}

		DB db = DBMaker
				.fileDB(dbFile)
//				.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.mmapFileEnable() //occur error when db is closed.
//				.cacheDisable() // workaround internal error
//				.commitFileSyncDisable()
//				.asyncWriteFlushDelay(100)
//				.lockSingleEnable()
				.closeOnJvmShutdown()
				.deleteFilesAfterClose()
				.make();
		db.compact();

		stringAndInteger(db, MULTIMAP_NAME_STRING_AND_INTEGER);

		stringAndByteArray(db, MULTIMAP_NAME_STRING_AND_BYTE_ARRAY);

		db.close();

	}

	private static void stringAndInteger(DB db, String mapName) {
		NavigableSet<Object[]> multiMap = db
				.createTreeSet(mapName)
				.serializer(BTreeKeySerializer.ARRAY2)
				.make();

		multiMap.add(new Object[] { "aa", 1 });
		multiMap.add(new Object[] { "aa", 2 });
		multiMap.add(new Object[] { "aa", 3 });
		multiMap.add(new Object[] { "bb", 1 });

		db.commit();

		Iterator<Object[]> it = multiMap.iterator();
		while (it.hasNext()) {

			Object[] tuple = it.next();

			System.out.println(tuple[0] + ", " + tuple[1]);
		}

		// find all values for a key
		for (Object[] l : Fun.filter(multiMap, "aa")) {
			System.out.println("value for key 'aa': " + l[1]);
		}
	}

//	1000 times add item and commit with each 	:   1724ms
//	1000 times add item all 					:     20ms
//	     and commit once 						:     15ms
//	1000 times search item 						:      5ms
//
//	10000 times add item and commit with each 	:  12480ms
//	10000 times add item all 					:     76ms
//	      and commit once 						:     74ms
//	10000 times search item 					:     26ms
//
//
//	100000 times add item and commit with each 	: 117204ms
//	100000 times add item all					:   1110ms
//	       and commit once 						:    584ms
//	100000 times search item 					:    210ms
	private static void stringAndByteArray(DB db, String mapName) {
		NavigableSet<Object[]> multiMap = db.createTreeSet(mapName)
//				.comparator(Fun.TUPLE2_COMPARATOR)
//				.comparator(BTreeMap.COMPARABLE_COMPARATOR)
//				.comparator(
//						new Tuple2Comparator<String, byte[]>(
//								BTreeMap.COMPARABLE_COMPARATOR,
//								Fun.BYTE_ARRAY_COMPARATOR))
//				 .serializer(BTreeKeySerializer.TUPLE2)
//				 .serializer(
//				 new Tuple2KeySerializer<String, byte[]>(
//				 BTreeMap.COMPARABLE_COMPARATOR,
//				 Serializer.STRING,
//				 Serializer.BYTE_ARRAY))
//				.serializer(BTreeKeySerializer.ARRAY2)
				.comparator(new Fun.ArrayComparator(
						new Comparator[]{Fun.COMPARATOR, Fun.BYTE_ARRAY_COMPARATOR}))
				.serializer(new ArrayKeySerializer(
						new Comparator[]{Fun.COMPARATOR, Fun.BYTE_ARRAY_COMPARATOR},
						new Serializer[]{Serializer.STRING, Serializer.BYTE_ARRAY}
						))
				.makeOrGet();
		

		int count = 1000;

//		Object[] obj = new Object[] {
//				Util.getRandomString(1, 100),
//				Util.getRandomByteArray(1, 100) };

		StopWatch.start();
		for (int i = 0; i < count; i++) {
			addItem(multiMap);
			db.commit();
		}
		StopWatch.stop(count + " times add item and commit with each");
		
		StopWatch.start();
		for (int i = 0; i < count; i++) {
			addItem(multiMap);
		}
		StopWatch.reset(count + " times add item all");
		db.commit();
		StopWatch.stop("and commit once");

		StopWatch.start();
		for (int i = 0; i < count; i++) {
			Fun.filter(multiMap, Util.getRandomString(1, 100));
		}
		StopWatch.stop(count + " times search item");
	}

	private static void addItem(NavigableSet<Object[]> multiMap, Object[] item) {
		multiMap.add(item);
	}
	
	private static void addItem(NavigableSet<Object[]> multiMap) {
		multiMap.add(new Object[] {
				Util.getRandomString(1, 100),
				Util.getRandomByteArray(1, 100) });
	}
}
