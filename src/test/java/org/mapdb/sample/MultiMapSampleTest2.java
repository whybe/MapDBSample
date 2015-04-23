package org.mapdb.sample;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.junit.Before;
import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.sun.javafx.collections.MappingChange.Map;

public class MultiMapSampleTest2 {
//	public static final String MULTIMAP_NAME_STRING_AND_INTEGER = "multimapsi";
//	public static final String MULTIMAP_NAME_STRING_AND_BYTE_ARRAY = "multimapsba";
//	public static final String MULTIMAP_NAME_5_STRING = "multimap5s";

	public static final String RESOURCE_PATH = "src/test/resources/org/mapdb/sample";
	public static final String DB_FILE = "multimap2.mapdb";
	
	private DB db;
	
	@Before
	public void setUp() throws Exception {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
//		if (dbFile.exists()) {
//			dbFile.delete();
//		}

		db = DBMaker.fileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
				//.cacheDisable() // workaround internal error
				//.transactionDisable()
				.snapshotEnable()
				.closeOnJvmShutdown()
				.deleteFilesAfterClose()
				.make();
//		db.compact();
	}

	@Test
	public void test() {
		// arrange
		BTreeMap<String, byte[]> map = db.getTreeMap("treemap");
		byte[] value = Util.getRandomByteArray(10);
		
		// act
		map.put("a", value);
		map.put("b", value);
		map.put("c", value);
		map.put("d", value);
		db.commit();

		// assert
		NavigableMap<String, byte[]>map2 = map.snapshot();
		assertArrayEquals(value, map2.get("a"));
		assertArrayEquals(value, map2.get("b"));
		assertArrayEquals(value, map2.get("c"));
		assertArrayEquals(value, map2.get("d"));
		String str = map2.get("b").getClass().getName();
		assertEquals("[B", str);
	}

}
