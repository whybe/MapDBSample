package org.mapdb.sample;

import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.mapdb.DB;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;

public class EnqueueThread extends QueueThread implements Runnable {

	public EnqueueThread(DB db, String queueName) {
		super(db, queueName);
	}

	public EnqueueThread(TxMaker tx, String queueName) {
		super(tx, queueName);
	}

	public EnqueueThread(DB db, BlockingQueue<String> queue) {
		super(db, queue);
	}

	public void run() {

		while (true) {
			if (tx != null)
				tx.execute(new TxBlock() {
					public void tx(DB db) throws TxRollbackException {
						String item = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 100));
						db.getQueue(queueName).add(item);
						//db.commit(); // it doesn't need.
						System.out.println("Enqueue - " + item);
						
						try {
							Thread.sleep(RandomUtils.nextInt(1, 100));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			else
			{
				String item = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 100));
				enqueue(item);

				try {
					Thread.sleep(RandomUtils.nextInt(1, 100));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void enqueue(String item) {
		queue.add(item);
		db.commit();

		System.out.println("Enqueue - " + item);
	}
}
