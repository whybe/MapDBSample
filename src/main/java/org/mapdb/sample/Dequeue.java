package org.mapdb.sample;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class Dequeue {
	private static final String QUEUE_NAME = "fifo";
	private final static String RESOURCE_PATH = "/Users/yb/Documents/MapDB/workspace/MapDBSample/src/main/resources/org/mapdb/sample";

	public static void main(String[] args) {
		File dbFile = new File(RESOURCE_PATH, "mapdb");
		DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().make();

		BlockingQueue<String> fifo = db.getQueue(QUEUE_NAME);


		String item;
		while ((item = fifo.poll()) != null) { 
			System.out.println(item);
		}

		db.commit();
		db.close();
	}

}
