package com.all.elasticsearch.producerconsumer;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.FileNotFoundException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasticsearch.client.Client;

public class MultiThreadedBulkWrite {

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 5) {
			System.err
					.println("usage : <filename> <numthreads> <bulkWritePerThread> <indice> <type>");
			System.exit(0);
		}

		String fileName = args[0];
		int numConsumers = Integer.parseInt(args[1]);
		int bulkSize = Integer.parseInt(args[2]);
		String index = args[3];
		String type = args[4];

		String[] header = new String[] { "id", "source_id", "item_id", "date",
				"time", "price", "shipping_cost", "availability",
				"price_change_count", "updated_at" };

		BlockingQueue<String> sharedBlockingQueue = new LinkedBlockingQueue<String>(
				bulkSize * numConsumers * 2);

		Thread prodThread = new Thread(new Producer(sharedBlockingQueue,
				fileName, bulkSize, numConsumers));
		Thread[] conThreads = new Thread[numConsumers];
		for (int ix = 0; ix < numConsumers; ix++) {
			Client client = nodeBuilder().clusterName("elasticsearch")
					.client(true).node().client();
			conThreads[ix] = new Thread(new Consumer(sharedBlockingQueue,
					client, index, type, header, bulkSize, ix));
		}

		prodThread.start();
		for (int ix = 0; ix < numConsumers; ix++) {
			conThreads[ix].start();
		}
	}
}
