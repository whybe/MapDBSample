package org.mapdb.sample;

import java.util.concurrent.BlockingQueue;

import org.mapdb.DB;
import org.mapdb.Serializer;
import org.mapdb.TxMaker;

public abstract class QueueThread implements Runnable {

	protected DB db;
	protected String queueName;
	protected TxMaker tx;
	protected BlockingQueue<String> queue;

	protected QueueThread(DB db, String queueName) {
		this.db = db;
		this.queueName = queueName;
		this.tx = null;
		this.queue = makeQueue(db, queueName);
	}

	protected QueueThread(TxMaker tx, String queueName) {
		this.db = null;
		this.tx = tx;
		this.queueName = queueName;
		this.queue = null;
	}

	public QueueThread(DB db, BlockingQueue<String> queue) {
		this.db = db;
		this.queue = queue;
	}

	public abstract void run();

	public static BlockingQueue<String> makeQueue(DB db, String queueName) {
		BlockingQueue<String> queue = db.get(queueName);
		if (queue == null) {
			queue = db.createQueue(queueName, Serializer.STRING, false);
		}

		return queue;
	}

	public static DB makeDb(TxMaker tx) {
		return tx.makeTx();
	}
}
