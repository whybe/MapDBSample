package org.mapdb.sample;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.NavigableSet;

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
	public static final String RESOURCE_PATH = "src/test/resources/org/mapdb/sample";
	public static final String DB_FILE = "multimap.mapdb";

	@Test
	public void MutimapFileCommitTestWithStringAndString() {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
//		if (dbFile.exists()) {
//			dbFile.delete();
//		}

		DB db = DBMaker.newFileDB(dbFile)
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

		DB db = DBMaker.newFileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
				//.cacheDisable() // workaround internal error
				//.transactionDisable()
				.closeOnJvmShutdown()
				.deleteFilesAfterClose()
				.make();
//		db.compact();

		NavigableSet<Object[]> multiMap = db
				.createTreeSet(MULTIMAP_NAME_STRING_AND_INTEGER)
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

}
