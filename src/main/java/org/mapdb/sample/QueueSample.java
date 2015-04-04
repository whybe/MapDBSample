package org.mapdb.sample;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;

public class QueueSample {

	public static final String QUEUE_NAME = "fifo";
	public static final String RESOURCE_PATH = "src/main/resources/org/mapdb/sample";
	public static final String DB_FILE = "fifo.mapdb";

	public static void main(String[] args) throws InterruptedException {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);

		// startQueueWithDb(dbFile);
		startQueueWithTx(dbFile);
		// startQueueWithQueue(dbFile);

	}

	private static void startQueueWithQueue(File dbFile) {
		DB db = DBMaker.newFileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.cacheDisable() // workaround internal error
				.closeOnJvmShutdown()
				.make();
		db.compact();

		BlockingQueue<String> queue = QueueThread.makeQueue(db, QUEUE_NAME);

		Thread enqueueThread = new Thread(new EnqueueThread(db, queue));
		Thread dequeueThread = new Thread(new DequeueThread(db, queue));

		enqueueThread.start();
		dequeueThread.start();
	}

	private static void startQueueWithTx(File dbFile) {
		TxMaker txMaker = DBMaker
				.newFileDB(dbFile)
//				.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.cacheDisable() // workaround internal error
				.closeOnJvmShutdown()
				.makeTxMaker();
		txMaker.execute(new TxBlock() {
			public void tx(DB db) throws TxRollbackException {
				db.compact();
			}
		});

		Thread enqueueThread = new Thread(
				new EnqueueThread(txMaker, QUEUE_NAME));
		Thread dequeueThread = new Thread(
				new DequeueThread(txMaker, QUEUE_NAME));
		enqueueThread.start();
		dequeueThread.start();
	}

	private static void startQueueWithDb(File dbFile) {
		DB db = DBMaker.newFileDB(dbFile)
				//.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.cacheDisable() // workaround internal error
				.closeOnJvmShutdown()
				.make();
		db.compact();

		Thread enqueueThread = new Thread(new EnqueueThread(db, QUEUE_NAME));
		Thread dequeueThread = new Thread(new DequeueThread(db, QUEUE_NAME));

		enqueueThread.start();
		dequeueThread.start();
	}

}
