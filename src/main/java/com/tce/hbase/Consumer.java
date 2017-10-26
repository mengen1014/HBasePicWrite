package com.tce.hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
/**
 * Title:多线程消费者
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午5:03:06
 */
public class Consumer extends Thread {

	private static final Logger logger = Logger.getLogger("runningLogger");
	private static final Logger uploadingLogger = Logger.getLogger("uploadingLogger");

	private List<String> fileList = new ArrayList<String>();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private AtomicBoolean running = new AtomicBoolean();
	private FileMonitor monitor;
	private FileMonitor testMonitor;
	private long count = 0; // 上传计数（不等于文件计数，因为每次可能上传多个文件）

	public Consumer() {

		monitor = new FileMonitor(this);
    	monitor.monitor(ServiceConfig.monitorDir, 50, ServiceConfig.fileSuffix);

    	if (ServiceConfig.testDir != null && !ServiceConfig.testDir.isEmpty()) {
    		testMonitor = new FileMonitor(this);
    		testMonitor.monitor(ServiceConfig.testDir, 50, ServiceConfig.fileSuffix);
    	}
	}

	@Override
	public void run() {

		running.set(true);

		while (running.get()) {
			try {
				consume();
			} catch (Exception e) {
				logger.error(String.format("Consumer thread exception: %s", e.getMessage()));
				e.printStackTrace();
			}
		}
	}

	public void cache(String filepath)
	{
		lock.writeLock().lock();
		fileList.add(0, filepath);
		lock.writeLock().unlock();
	}

	private void consume() {

		lock.writeLock().lock();
		List<String> batch = new ArrayList<String>();
		for (int index = 0; index < ServiceConfig.maxFiles; index++) {
			if (fileList.size() > 0) {
				//取出第一个，类似于管道取出第一位，后面的成为新的第一位【Dequeue】队列
				String filepath = fileList.remove(0);
				logger.info(String.format("Dequeue %d: %s", index, filepath));
				batch.add(filepath);
			}
		}
		lock.writeLock().unlock();

		try {
			if (batch.size() == 0) {
				Thread.sleep(50);
				return;
			} else {
				Thread.sleep(ServiceConfig.uploadInterval);					
			}
		} catch (InterruptedException e) {
			logger.warn("Consumer thread sleep was interrupted");
			e.printStackTrace();
			return;
		}

		count++;
		long start = System.currentTimeMillis();
		boolean result = HBaseWrapper.getInstance().upload(batch);
		long time = System.currentTimeMillis() - start;
		if (result) {
			logger.info(String.format("Success, count: %d, files: %d, time: %dms", count, batch.size(), time));
			for (String path: batch) {
				uploadingLogger.info(String.format("current|%d|%dms|%s|0", count, time, path));

				if (ServiceConfig.enableDelete) {
					try {
						File file = new File(path);
						if (file.delete()) {
							logger.error(String.format("Succeeded delete %s", path));
						} else {
							logger.error(String.format("Failed delete %s", path));	
						}
						file = null;
					} catch (Exception e) {
						logger.error(String.format("Failed delete %s", path));
					}
				}
			}
		} else {
			logger.info(String.format("Failure, count: %d, files: %d, time: %dms", count, batch.size(), time));
			for (String path: batch) {
				uploadingLogger.info(String.format("current|%d|%dms|%s|1", count, time, path));
			}
		}
	}
}
