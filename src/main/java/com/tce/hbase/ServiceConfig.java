package com.tce.hbase;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
/**
 * Title:读取配置文件
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午4:59:19
 */
public class ServiceConfig {
	public static String localIP;
	public static int checkInterval = 10;

	public static int uploadInterval;
	public static int minDelay = 0;
	public static int maxFiles;
	public static String zookeeperUrl;
	public static String tableName;
	public static String monitorDir;
	public static String fileSuffix;
	public static String testDir;
	public static boolean enableDelete;
	public static boolean rowKeySlash;

	private static final Logger logger = Logger.getLogger("runningLogger");

	public static void configure(String fileName) throws Exception {
		logger.debug(String.format("Loading config from %s", fileName));
		
		InputStream stream = new FileInputStream(fileName);
    	Properties props = new Properties();
        props.load(stream);

        uploadInterval = Integer.parseInt(props.getProperty("uploadInterval"));
        logger.debug(String.format("Upload Interval: %d", uploadInterval));
        
        maxFiles = Integer.parseInt(props.getProperty("maxFiles"));
        logger.debug(String.format("Max Files: %d", maxFiles));

        zookeeperUrl = props.getProperty("zookeeperUrl");
        logger.debug(String.format("Zookeeper Url: %s", zookeeperUrl));
        
        tableName = props.getProperty("tableName");
        if (tableName == null || tableName.isEmpty()) {
        	throw new Exception(String.format("Invalid Table Name: %s", tableName));
        } else {
        	logger.debug(String.format("Table Name: %s", tableName));
        }

        monitorDir = props.getProperty("monitorDir");
        logger.debug(String.format("Monitor Dir: %s", monitorDir));

        fileSuffix = props.getProperty("fileSuffix");
        logger.debug(String.format("File Suffix: %s", fileSuffix));
        
        testDir = props.getProperty("testDir");
        logger.debug(String.format("Test Dir: %s", testDir));
        
        enableDelete = props.getProperty("enableDelete").equals("true");
        logger.debug(String.format("Enable Delete: %s", enableDelete));
        
        rowKeySlash = props.getProperty("rowKeySlash").equals("true");
        logger.debug(String.format("Row Key Slash: %s", rowKeySlash));
	}
}
