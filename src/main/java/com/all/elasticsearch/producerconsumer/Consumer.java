package com.all.elasticsearch.producerconsumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class Consumer implements Runnable {

	private static AtomicBoolean queueOver = new AtomicBoolean(false);

	private int threadID;
	private Client client;
	public String[] header;
	private String index, type;
	private int bulkWriteSize;
	private final int retries = 3;
	public static long timer = -1;
	private static BlockingQueue<String> sharedBlockingQueue;
	private BulkRequestBuilder bulkRequest;
	private Logger logger;
	private String threadInformation;

	public Consumer(BlockingQueue<String> sharedBlockingQueue, Client client,
			String index, String type, String[] header, int bulkWriteSize,
			int threadId) {
		Consumer.sharedBlockingQueue = sharedBlockingQueue;
		this.client = client;
		this.index = index;
		this.type = type;
		this.header = header;
		this.bulkWriteSize = bulkWriteSize;
		this.threadID = threadId;
		if (timer == -1)
			timer = System.currentTimeMillis();
		logger = Logger.getLogger(this.getClass().toString());
		threadInformation = " Thread ID " + threadId;
		bulkRequest = client.prepareBulk();
	}

	public void run() {

		while (true) {
			try {
				String line = fetchData();
				if (line == null)
					break;

				processData(line);

				if (bulkRequest.numberOfActions() == bulkWriteSize) {
					insert(bulkRequest);
					bulkRequest = null;
					bulkRequest = client.prepareBulk();
				}

			} catch (InterruptedException e) {
				logger.log(Level.SEVERE, "Thread " + threadID + " Error", e);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (bulkRequest != null && bulkRequest.numberOfActions() != 0) {
			insert(bulkRequest);
		}
		logger.log(Level.INFO, threadInformation + " exiting");

	}

	public static synchronized String fetchData() throws InterruptedException {
		if (queueOver.get())
			return null;

		String str = sharedBlockingQueue.take();
		if (str.equals(Producer.NULL)) {
			queueOver.set(true);
			str = null;
		}
		return str;
	}

	public void processData(String line) throws IOException {

		int jx = 0;
		StringTokenizer st = new StringTokenizer(line, "\t");
		XContentBuilder tmp = jsonBuilder().startObject();
		String id = null;
		printLog();
		while (st.hasMoreTokens()) {
			if (header[jx].equals("id")) {
				id = st.nextToken();
				tmp.field(header[jx], id);
			} else
				tmp.field(header[jx], st.nextToken());
			jx++;
		}

		bulkRequest.add(client.prepareIndex(index, type).setId(id)
				.setSource(tmp));
	}

	public synchronized void printLog() {
		if (System.currentTimeMillis() - timer > 10000) {
			logger.log(Level.INFO, this.getClass().toString()
					+ " Elements left in queue " + sharedBlockingQueue.size());
			timer = System.currentTimeMillis();
		}
	}

	public boolean insert(BulkRequestBuilder bulkRequest) {
		boolean insert = false;
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			int ix = 0;
			boolean success = false;
			for (ix = 0; ix < retries; ix++) {
				bulkResponse = bulkRequest.execute().actionGet();
				if (!bulkResponse.hasFailures()) {
					success = true;
					insert = true;
					break;
				}
			}
			if (!success) {
				logger.log(Level.SEVERE, "Bulk Insert Failed after " + retries
						+ " retries " + threadInformation);
			} else {
				logger.log(Level.SEVERE, "Bulk insert success after " + ix
						+ "tries " + threadInformation);
			}
		} else {
			logger.log(Level.INFO, "Inserted " + bulkRequest.numberOfActions()
					+ " Documents " + threadInformation);
			insert = true;
		}
		return insert;
	}

	public static void main(String[] args) throws InterruptedException {
		BlockingQueue<String> tmp = new LinkedBlockingQueue<String>();
		tmp.put(null);
	}
}
