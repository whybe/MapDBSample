package org.mapdb.sample;

import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.RandomUtils;
import org.mapdb.DB;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;

public class DequeueThread extends QueueThread implements Runnable {

	public DequeueThread(DB db, String queueName) {
		super(db, queueName);
	}

	public DequeueThread(TxMaker txMaker, String queueName) {
		super(txMaker, queueName);
	}

	public DequeueThread(DB db, BlockingQueue<String> queue) {
		super(db, queue);
	}

	public void run() {

		while (true) {
			if (tx != null)
				tx.execute(new TxBlock() {
					public void tx(DB db) throws TxRollbackException {
						String item = (String) db.getQueue(queueName).poll();
						if (item != null) {
							//db.commit(); // it doesn't need.
							System.out.println("Dequeue - " + item);
						}

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
				dequeue();

				try {
					Thread.sleep(RandomUtils.nextInt(1, 100));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void dequeue() {
		String item = queue.poll();
		if (item != null) {
			db.commit();

			System.out.println("Dequeue - " + item);
		}
	}
}
