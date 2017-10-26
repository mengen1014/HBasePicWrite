package com.tce.hbase;

import java.io.File;
import java.util.TimeZone;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
/**
 * Title:文件监控类
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午10:16:36
 * 继承 FileAlterationListenerAdaptor
 * common-io-2.0的新功能之文件监控FileAlterationListenerAdaptor
 */
public class FileMonitor extends FileAlterationListenerAdaptor {

	private static final Logger logger = Logger.getLogger("runningLogger");
	private static FileMonitor fileMonitor;
	private Producer producer = null;
	private Consumer consumer = null;
	private FileAlterationMonitor fileAlterationMonitor = null;

	private FileMonitor() {
	}
	
	public FileMonitor(Producer producer) {
		this.producer = producer;
	}
	
	public FileMonitor(Consumer consumer) {
		this.consumer = consumer;
	}

	// Get singleton object instance
	public static FileMonitor getFileMonitor() {
		if (fileMonitor == null) {
			synchronized (FileMonitor.class) {
				if (fileMonitor == null) {
					fileMonitor = new FileMonitor();
				}
			}
		}

		return fileMonitor;
	}

	// Create file event
	@Override
	public void onFileCreate(File file) {
		String fileAbsolutePath = file.getAbsolutePath();
		logger.info("[Create]: " + fileAbsolutePath);

//		String fileAbsolutePath = file.getAbsolutePath();
//		String fileAbsoluteParentPath = file.getParent();
//		String fileBaseName = FilenameUtils.getBaseName(fileAbsolutePath);
//
//		logger.info("fileAbsolutePath:" + fileAbsolutePath);
//		logger.info("fileAbsoluteParentPath:" + fileAbsoluteParentPath);
//		logger.info("fileBaseName:" + fileBaseName);
		//当文件创建时缓存
		if (producer != null) {
			logger.info(String.format("Caching %s to producer", fileAbsolutePath));
			producer.cache(fileAbsolutePath);
		}
		
		if (consumer != null) {
			logger.info(String.format("Pushing %s to consumer", fileAbsolutePath));
			consumer.cache(fileAbsolutePath);
		}
	}

	// Change file event
	@Override
	public void onFileChange(File file) {
		String absolutePath = file.getAbsolutePath();
		logger.info("[Change]: " + absolutePath);
		
		if (producer != null) {
			logger.info(String.format("Caching %s to producer again!", absolutePath));
			producer.cache(absolutePath);
		}
	}

	// Delete file event
	@Override
	public void onFileDelete(File file) {
		logger.info("[Delete]: " + file.getAbsolutePath());
	}

	public boolean stop() {
		if (fileAlterationMonitor != null) {
			try {
				fileAlterationMonitor.stop();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	public void monitor(String directory, int interval, String suffix) {
		logger.info("监听目录：" + directory);
		// Observer file whose suffix is pm
		FileAlterationObserver fileAlterationObserver = new FileAlterationObserver(directory,
				FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter(suffix)), null);

		// Add listener for event (file create & change & delete)
		fileAlterationObserver.addListener(this);

		// Monitor per interval
		fileAlterationMonitor = new FileAlterationMonitor(interval, fileAlterationObserver);

		try {
			// Start to monitor
			fileAlterationMonitor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)
	{
		TimeZone.setDefault(TimeZone.getTimeZone("Asia/Chongqing"));
    	PropertyConfigurator.configure("conf/log4j.properties");
		FileMonitor.getFileMonitor().monitor("E:\\webapp\\photoftp\\photoback\\20170621\\19", 555, ".jpg");
	}
}
