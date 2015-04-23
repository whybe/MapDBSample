package org.mapdb.sample;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeKeySerializer.ArrayKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;

@RunWith(JUnitParamsRunner.class)
public class MultiMapSampleTest {

	public static final String MULTIMAP_NAME_STRING_AND_INTEGER = "multimapsi";
	public static final String MULTIMAP_NAME_STRING_AND_BYTE_ARRAY = "multimapsba";
	public static final String MULTIMAP_NAME_5_STRING = "multimap5s";

	public static final String RESOURCE_PATH = "src/test/resources/org/mapdb/sample";
	public static final String DB_FILE = "multimap.mapdb";

	@Test
	public void MutimapFileCommitTestWithStringAndString() {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
//		if (dbFile.exists()) {
//			dbFile.delete();
//		}

		DB db = DBMaker.fileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
				//.cacheDisable() // workaround internal error
				//.transactionDisable()
				.closeOnJvmShutdown()
				.deleteFilesAfterClose()
				.make();
//		db.compact();

		NavigableSet<Object[]> multiMap = db
				.createTreeSet(MULTIMAP_NAME_STRING_AND_INTEGER)
				.serializer(BTreeKeySerializer.ARRAY2)
				.makeOrGet();

		String key = "key";
		String value = "value";

		multiMap.add(new Object[] { key, value });

		db.commit();

		assertEquals(true, multiMap.contains(new Object[] { key, value }));
		assertEquals(value, Fun.filter(multiMap, key).iterator().next()[1]);

		db.close();
	}

	private static final Object[] getItemsWithStringAndByteArray() {
//		return new Object[] {
//				new Object[] {
//						getRandomString(1, 100),
//						getRandomByteArray(1, 100)
//				},
//				new Object[] {
//						getRandomString(1, 100),
//						getRandomByteArray(1, 100)
//				}
//		};

		ArrayList<Object> objArrayList = new ArrayList<Object>();

		int loop = 10;
		while (loop-- > 0)
		{
			objArrayList.add(new Object[] {
					Util.getRandomString(1, 100),
					Util.getRandomByteArray(1, 100) });
		}

		return objArrayList.toArray();

	}

	@Test
	@Parameters(method = "getItemsWithStringAndByteArray")
	public void mutimapFileCommitTestWithStringAndByteArray(String key, byte[] value) {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
//		if (dbFile.exists()) {
//			dbFile.delete();
//		}

		DB db = DBMaker.fileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
				//.cacheDisable() // workaround internal error
				//.transactionDisable()
				.closeOnJvmShutdown()
				.deleteFilesAfterClose()
				.make();
//		db.compact();

		NavigableSet<Object[]> multiMap = db
				.createTreeSet(MULTIMAP_NAME_STRING_AND_BYTE_ARRAY)
				.comparator(
						new Fun.ArrayComparator(
								new Comparator[] { Fun.COMPARATOR, Fun.BYTE_ARRAY_COMPARATOR }))
				.serializer(
						new ArrayKeySerializer(
								new Comparator[] { Fun.COMPARATOR, Fun.BYTE_ARRAY_COMPARATOR },
								new Serializer[] { Serializer.STRING, Serializer.BYTE_ARRAY }))
				//.serializer(BTreeKeySerializer.ARRAY2) // not work
				.makeOrGet();

		multiMap.add(new Object[] { key, value });
		db.commit();

		assertEquals(true, multiMap.contains(new Object[] { key, value }));
		assertArrayEquals(value, (byte[]) Fun.filter(multiMap, key).iterator().next()[1]);

		db.close();
	}

	private static final Object[] getItemsWithFourKeysAndOneValue() {
		return new Object[] {
				new Object[] { "a1", "b1", "c1", "d1", "v0" },
				new Object[] { "a1", "b1", "c1", "d1", "v1" }, // a, b, c, d
				new Object[] { "a2", "b1", "c2", "d2", "v2" }, // b
				new Object[] { "a3", "b3", "c1", "d3", "v3" }, // c
				new Object[] { "a4", "b4", "c4", "d1", "v4" }, // d
				new Object[] { "a4", "b5", "c5", "d5", "v5" }, // a
				new Object[] { "a6", "b5", "c6", "d6", "v6" }, // b 
				new Object[] { "a7", "b7", "c6", "d7", "v7" }, // c
				new Object[] { "a8", "b8", "c8", "d7", "v8" }, // d
				new Object[] { "a9", "b9", "c2", "d9", "v9" }  // c
		};
	}

	@Test
	public void mutimapFilterTest() {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
//		if (dbFile.exists()) {
//			dbFile.delete();
//		}

		DB db = DBMaker.fileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
				//.cacheDisable() // workaround internal error
				//.transactionDisable()
				.closeOnJvmShutdown()
				//				.deleteFilesAfterClose()
				.make();
//		db.compact();

		NavigableSet<Object[]> multiMap = db
				.createTreeSet(MULTIMAP_NAME_5_STRING)
				.comparator(
						new Fun.ArrayComparator(
								new Comparator[] {
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR }))
				.serializer(
						new ArrayKeySerializer(
								new Comparator[] {
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR,
										Fun.COMPARATOR },
								new Serializer[] {
										Serializer.STRING,
										Serializer.STRING,
										Serializer.STRING,
										Serializer.STRING,
										Serializer.STRING }))
				.makeOrGet();

		multiMap.add(new Object[] { "a1", "b1", "c1", "d1", "v1" });
		multiMap.add(new Object[] { "a2", "b2", "c2", "d2", "v2" });
		multiMap.add(new Object[] { "a3", "b3", "c3", "d3", "v3" });
		multiMap.add(new Object[] { "a4", "b4", "c4", "d4", "v4" });
		db.commit();

		assertEquals(true, multiMap.contains(new Object[] { "a1", "b1", "c1", "d1", "v1" }));
		assertEquals(true, multiMap.contains(new Object[] { "a2", "b2", "c2", "d2", "v2" }));
		assertEquals(true, multiMap.contains(new Object[] { "a3", "b3", "c3", "d3", "v3" }));
		assertEquals(true, multiMap.contains(new Object[] { "a4", "b4", "c4", "d4", "v4" }));
		assertEquals("v1", Fun.filter(multiMap, "a1").iterator().next()[4]);
		assertEquals("v2", Fun.filter(multiMap, "a2").iterator().next()[4]);
		assertEquals("v3", Fun.filter(multiMap, "a3").iterator().next()[4]);
		assertEquals("v4", Fun.filter(multiMap, "a4").iterator().next()[4]);
		
		SortedSet<Object[]> sortedSet = multiMap.tailSet(new Object[] {"a1"});
		
		Iterator<Object[]> iter = Fun.filter(multiMap, "a1").iterator();
		int count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(1, count);
		
		iter = Fun.filter(multiMap, "a1", "b1").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(1, count);
		
		iter = Fun.filter(multiMap, "a1", "b1", "c1").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(1, count);
		
		iter = Fun.filter(multiMap, "a1", "b1", "c1", "d1").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(1, count);
		

		
		iter = Fun.filter(multiMap, "a1", "c1").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(0, count);
		
		iter = Fun.filter(multiMap, "a1", "b2").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(0, count);
		
		iter = Fun.filter(multiMap, "b1").iterator();
		count = 0;
		while(iter.hasNext()) {
			count++;
			iter.next();
		}
		assertEquals(0, count);
		
		

		db.close();
	}

}
