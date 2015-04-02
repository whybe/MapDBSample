package org.mapdb.sample;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class Enqueue {
	private static final String QUEUE_NAME = "fifo";
	private final static String RESOURCE_PATH = 
			"/Users/yb/Documents/MapDB/workspace/MapDBSample/src/main/resources/org/mapdb/sample";

	public static void main(String[] args) throws IOException {

		File dbFile = new File(RESOURCE_PATH, "mapdb");
		DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().make();
		
		db.checkNameNotExists(QUEUE_NAME);

		
//		BlockingQueue<String> fifo = db.createQueue(QUEUE_NAME, Serializer.STRING, true);
		BlockingQueue<String> fifo = db.get(QUEUE_NAME);
		if (fifo == null) {
			fifo = db.createQueue(QUEUE_NAME, Serializer.STRING, true);
		}

		fifo.add("one");
		fifo.add("tow");
		
		db.commit();
		
		fifo.add("three");
		
		db.rollback();
		
		db.commit();
		
		db.close();

		// Configure and open database using builder pattern.
		// All options are available with code auto-completion.
//		File dbFile = File.createTempFile("mapdb", "db");		
		// open an collection, TreeMap has better performance then HashMap
//		ConcurrentNavigableMap<Integer, String> map = db
//				.getTreeMap("collectionName");
//		
//		map.put(1, "one");
//		map.put(2, "two");
		// map.keySet() is now [1,2] even before commit

//		db.commit(); // persist changes into disk

//		map.put(3, "three");
		// map.keySet() is now [1,2,3]
//		db.rollback(); // revert recent changes
		// map.keySet() is now [1,2]

//		db.close();
	}

}
