package com.tce.hbase;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
/**
 * Title:生产方
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午10:44:47
 * 主要作用是在监控文件夹写入以及修改文件发生时，对该文件缓存
 */
public class Producer  extends Thread {

	private Consumer consumer;
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private FileMonitor monitor;
	private FileMonitor testMonitor;
	private Map<String, Long> cacheFiles = new HashMap<String, Long>();
	/**
	 * AtomicBoolean是java.util.concurrent.atomic包下的原子变量
	 * 在多线程下具有排他性
	 */
	public AtomicBoolean running = new AtomicBoolean();
	private static final Logger logger = Logger.getLogger("runningLogger");

    public Producer(Consumer consumer) {
    	this.consumer = consumer;

    	monitor = new FileMonitor(this);
    	monitor.monitor(ServiceConfig.monitorDir, 1000, ServiceConfig.fileSuffix);
    	
    	if (ServiceConfig.testDir != null && !ServiceConfig.testDir.isEmpty()) {
    		testMonitor = new FileMonitor(this);
    		testMonitor.monitor(ServiceConfig.testDir, 1000, ServiceConfig.fileSuffix);
    	}
    }
 
    @Override
    public void run() {
    	running.set(true);
    	while (running.get()) {

    		produce();

    		try {
				Thread.sleep(ServiceConfig.checkInterval);
			} catch (InterruptedException e) {
				logger.debug("Producer thread sleep was interrupted");
				e.printStackTrace();
				break;
			}
    	}
    }

    void cache(String filePath) {
    	lock.writeLock().lock();
    	long deadline = System.currentTimeMillis() + ServiceConfig.minDelay;
    	cacheFiles.put(filePath, deadline);
    	lock.writeLock().unlock();
    }

    void produce() {

    	lock.writeLock().lock();

    	List<String> pathsToRemove = new ArrayList<String>();
    	for (Map.Entry<String, Long> entry: cacheFiles.entrySet()) {
    		String path = entry.getKey();
    		//在写入HBase的过程中如果有新文件写入
    		long deanline = cacheFiles.get(path);
    		if (System.currentTimeMillis() > deanline) {
    			logger.info(String.format("Moving %s to consumer", path));
    			//缓存 到消费类
    			consumer.cache(path);
    			pathsToRemove.add(path);
    		}
    	}

    	for (String path: pathsToRemove) {
    		cacheFiles.remove(path);
    	}

		lock.writeLock().unlock();
    }
}