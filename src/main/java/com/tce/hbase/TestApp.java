package com.tce.hbase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TestApp {

	private static final Logger logger = Logger.getLogger("runningLogger");

    static {
//    	TimeZone.setDefault(TimeZone.getTimeZone("Asia/Chongqing"));
    	PropertyConfigurator.configure("conf/log4j.properties");
    }

	public static void main(String[] args) throws InterruptedException {

		logger.debug("HBase Uploader Start");

		try {
			ServiceConfig.configure("conf/hbase-uploader.properties");
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
			return;
		}

		try {
			logger.debug("Initializing hbase");
			HBaseWrapper.getInstance().init();
		} catch (Exception e) {
			logger.error("Failed initializing hbase");
			e.printStackTrace();
			return;
		}
		
		if (filterArgs(args)) {
			return;
		}

        logger.info("**********************************");
		logger.info("**                              **");
		logger.info("**             启动成功          **");
		logger.info("**                              **");
		logger.info("**********************************");

		byte[] content = null;
		String filepath = args[0];
		if (filepath == null) {
			logger.error("No file to upload");
			return;
		}

		try {
			InputStream input = new FileInputStream(filepath);
			content = IOUtils.toByteArray(input);
		} catch (FileNotFoundException e) {
			logger.error(String.format("No such file %s", filepath));
			e.printStackTrace();
			return;
		} catch (IOException e) {
			logger.error(String.format("Failed reading file %s", filepath));
			e.printStackTrace();
			return;
		}

		long count = 0;
		while (true) {
			Thread.sleep(ServiceConfig.uploadInterval);
			long start = System.currentTimeMillis();

			if (HBaseWrapper.getInstance().upload(filepath, content, ServiceConfig.maxFiles)) {
//				logger.info(String.format("Succeeded uploading %s", filepath));
			} else {
				logger.info(String.format("Failed uploading %s", filepath));
			}

			logger.info(String.format("%d,%d", ++count, System.currentTimeMillis() - start));
		}
	}
	
	public static boolean filterArgs(String[] args) {
		switch (args.length) {
		case 2: {
			String command = args[0];
			String parameter = args[1];
			logger.info(String.format("Executing command: %s %s", command, parameter));

			if (command.equals("create")) {
				HBaseWrapper.getInstance().createTable(parameter);
				return true;
			} else if (command.equals("list")) {
				HBaseWrapper.getInstance().list(parameter);
				return true;
			} else if (command.equals("reupload")) {
				HBaseWrapper.getInstance().reupload(parameter);
				return true;
			}
			break;
		}
		case 3: {
			String command = args[0];
			String parameter1 = args[1];
			String parameter2 = args[2];
			logger.info(String.format("Executing command: %s %s %s", command, parameter1, parameter2));

			if (command.equals("download")) {
				HBaseWrapper.getInstance().download(parameter1, parameter2);
				return true;
			} else if (command.equals("reupload")) {
				HBaseWrapper.getInstance().reupload(parameter1, parameter2);
				return true;
			}
			break;
		}
		default:
			break;
		}

		return false;
	}
}
