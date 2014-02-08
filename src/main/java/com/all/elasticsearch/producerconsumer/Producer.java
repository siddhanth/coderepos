package com.all.elasticsearch.producerconsumer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Producer implements Runnable {
	private final BlockingQueue<String> sharedQueue;
	private final BufferedReader file;
	private final int perConsumerRead;
	private final int numConsumers;
	public static volatile boolean IS_EMPTY;
	public static final String NULL = "EOF";
	private Logger logger;

	public Producer(BlockingQueue<String> shareQueue, String fileName,
			int perConsumerRead, int numConsumers) throws FileNotFoundException {
		this.sharedQueue = shareQueue;
		file = new BufferedReader(new FileReader(fileName));
		this.perConsumerRead = perConsumerRead;
		this.numConsumers = numConsumers;
		logger = Logger.getLogger(this.getClass().toString());
		IS_EMPTY = false;
	}

	public void run() {
		String line;
		try {
			while ((line = file.readLine()) != null) {
				try {
					putData(line);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			putData(NULL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.log(Level.INFO, this.getClass() + " finished reading ");
	}

	public synchronized void putData(String line) throws InterruptedException {
		sharedQueue.put(line);
	}

	public int getNumConsumers() {
		return numConsumers;
	}

	public int getPerConsumerRead() {
		return perConsumerRead;
	}
}
