package com.tce.hbase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
/**
 * Title:项目主入口
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午4:59:44
 * 
 * 代码由 TCE 重庆 徐彬 编写
 * 注释添加 TCE-MengEn.Cao
 * 代码水平可以值得学习
 * 
 */
public class App {

	private static final Logger logger = Logger.getLogger("runningLogger");
	private static final Logger uploadingLogger = Logger.getLogger("uploadingLogger");
	private static long historyCount = 0;

	//读取日志文件
    static {
    	PropertyConfigurator.configure("../conf/log4j.properties");
    }

	public static void main(String[] args) throws InterruptedException {

		logger.debug("HBase Uploader Start");

		//读取配置文件
		try {
			ServiceConfig.configure("../conf/application.properties");
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return;
		}
		//项目HBase环境检测
		try {
			logger.debug("Initializing hbase");
			HBaseWrapper.getInstance().init();
		} catch (Exception e) {
			logger.error("Failed initializing hbase");
			e.printStackTrace();
			return;
		}
		//多线程批量上传启动
		Consumer consumer = new Consumer();
		consumer.start();

        logger.info("**********************************");
		logger.info("**                              **");
		logger.info("**             启动成功         **");
		logger.info("**                              **");
		logger.info("**********************************");

		long start = System.currentTimeMillis();
		logger.info(String.format("Start uploading history files in %s", ServiceConfig.monitorDir));
		File[] files = null;
		try {
			File dir = new File(ServiceConfig.monitorDir);
			files = dir.listFiles();
		} catch (Exception e) {
			logger.error(String.format("Failed construct folder object: %s", e.getMessage()));
			e.printStackTrace();
		}
		//检查上传失败的文件再次上传
		checkHistory(files);
		files = null;
		long time = System.currentTimeMillis() - start;
		logger.info(String.format("Finished uploading history files, time: %dms", time));

		Thread.sleep(Integer.MAX_VALUE);
//		Thread.sleep(1000 * 60 * 5);
//		producer.running.set(true);
//		consumer.running.set(true);
//		producer.join();
//		consumer.join();

		logger.debug("Onest Photo Uploader Stop");
	}

	private static void checkHistory(File[] files) {
		//文件夹为空跳出
		if (files == null) {
			logger.info(String.format("Dir %s has no files for now", ServiceConfig.monitorDir));
			return;
		}
		//有效文件集合
		List<String> validFiles = new ArrayList<String>(); 
		for (File file: files) {
			String filename = file.getName();
			// 上传成功会把.ok 删掉（replaceAll("\\.ok", "")）
			//文件无效或不属于设定后缀文件跳过继续
			// .   表示本目录下 
			// ..  上一级目录
			if (filename.equals(".") || filename.equals("..") || !filename.endsWith(ServiceConfig.fileSuffix)) {
				logger.info(String.format("Invalid history file %s", filename));
				continue;
			}

			logger.info(String.format("Found a history file %s", filename));
			//获取文件绝对路径添加入集合中
			validFiles.add(file.getAbsolutePath());
		}
		//文件缓存集合
		List<String> batch = new ArrayList<String>();
		for (int index = 0; index < validFiles.size(); index++) {
			batch.add(validFiles.get(index));
			//当缓存文件大于单次上传数或者总文件数小于单次上传数也进入
			if (batch.size() >= ServiceConfig.maxFiles || index == validFiles.size() - 1) {

				historyCount++;
				long start = System.currentTimeMillis();
				//多文件上传
				boolean result = HBaseWrapper.getInstance().upload(batch);
				long time = System.currentTimeMillis() - start;
				if (result) {
					logger.info(String.format("Success, historyCount: %d, files: %d, time: %dms", historyCount, batch.size(), time));
					for (String path: batch) {
						uploadingLogger.info(String.format("history|%d|%dms|%s|0", historyCount, time, path));
						//本地文件是否删除
						if (ServiceConfig.enableDelete) {
							try {
								File fileToDelete = new File(path);
								if (fileToDelete.delete()) {
									logger.error(String.format("Succeeded delete %s", path));
								} else {
									logger.error(String.format("Failed delete %s", path));
								}
								fileToDelete = null;
							} catch (Exception e) {
								logger.error(String.format("Failed delete %s", path));
							}
						}
					}
				} else {
					logger.info(String.format("Failure, historyCount: %d, files: %d, time: %dms", historyCount, batch.size(), time));
					for (String path: batch) {
						uploadingLogger.info(String.format("history|%d|%dms|%s|1", historyCount, time, path));
					}
				}
				//清空集合
				batch.clear();
			}
		}
	}
}
