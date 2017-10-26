package com.tce.hbase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
/**
 * Title:HBase 封装类
 * Author:TCE-MengEn.Cao
 * Date：2017年10月26日 下午10:38:36
 *
 */
public class HBaseWrapper {

	private Configuration config;
	private Connection conn;
	private Admin admin;
	private long count = 0;
	//提供算法用于hbase key 加密
	MessageDigest digest = null;

	private static final Logger logger = Logger.getLogger("runningLogger");
	private static final Logger uploadingLogger = Logger.getLogger("uploadingLogger");
	private static HBaseWrapper instance = null;
	private static String os = System.getProperty("os.name").toLowerCase();

	//创建实例
	public static HBaseWrapper getInstance() {
		if (instance == null) {
			instance = new HBaseWrapper();
		}
		return instance;
	}

	private HBaseWrapper() {
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			digest = null;
		}
	}

	public void init() throws Exception {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ServiceConfig.zookeeperUrl);

		try {
			conn = ConnectionFactory.createConnection(config);
			admin = conn.getAdmin();
			logger.info("数据库连接成功");
			
//			logger.info("HBase configs:");
//			logger.info(String.format("hbase.hregion.memstore.flush.size: %s", config.get("hbase.hregion.memstore.flush.size")));
		} catch (IOException e) {
			logger.error("数据库连接失败");
			e.printStackTrace();
			throw e;
		}

		TableName table = TableName.valueOf(ServiceConfig.tableName);
		try {
			logger.info(String.format("Checking if '%s' exists", ServiceConfig.tableName));
			if (!admin.tableExists(table)) {
				throw new Exception(String.format("Table '%s' doesn't exist", ServiceConfig.tableName));
			}
		} catch (IOException e) {
			logger.error("Failed checking table's existence");
			e.printStackTrace();
			throw e;
		}

		HTableDescriptor desc = admin.getTableDescriptor(table);
		HColumnDescriptor[] columns = desc.getColumnFamilies();
		boolean pictureColumn = false;
		boolean descColumn = false;
		for (HColumnDescriptor column: columns) {
			String family = new String(column.getName());
			if (family.equals("picture")) {
				pictureColumn = true;
			} else if (family.equals("desc")) {
				descColumn = true;
			}
		}

		if (!pictureColumn) {
			throw new Exception(String.format("Table '%s' doesn't have column family 'picture'", ServiceConfig.tableName));
		}
		if (!descColumn) {
			throw new Exception(String.format("Table '%s' doesn't have column family 'desc'", ServiceConfig.tableName));
		}

    	logger.debug("Initialized hbase");
	}

	public boolean download(String path) {
		logger.info("Try downloading " + path);
		
		String filename = null;
		int slashPos = path.lastIndexOf(os.indexOf("linux") >= 0 ? "/" : "\\");
		if (slashPos >= 0) {
			filename = path.substring(slashPos + 1);
		} else {
			filename = path;
		}

		String[] tokens = filename.split("_");
		if (tokens.length != 6) {
			logger.warn(String.format("Invalid filename: %s", filename));
			return false;
		}

		String row = null;
		if (ServiceConfig.rowKeySlash) {
			row = String.format("/%s/%s/%s/%s/%s", tokens[1], tokens[2], tokens[3], tokens[4], tokens[5].replaceAll("\\.ok", ""));
		} else {
			row = String.format("%s/%s/%s/%s/%s", tokens[1], tokens[2], tokens[3], tokens[4], tokens[5].replaceAll("\\.ok", ""));
		}

		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(ServiceConfig.tableName));
		} catch (IOException e) {
			logger.error("Failed getting the table");
			e.printStackTrace();
			return false;
		}

		Get get = new Get(row.getBytes());
		try {
			Result result = table.get(get);
			byte[] value = result.getValue("picture".getBytes(), "picture".getBytes());
			String filepath = String.format("downloads/%s", filename);
			FileUtils.writeByteArrayToFile(new File(filepath), value);
			return true;
		} catch (IOException e) {
			logger.info(String.format("Failed download file: %s", e.getMessage()));
			e.printStackTrace();
		}
		return false;
	}

	public boolean createTable(String name) {
		TableName tableName = TableName.valueOf(name);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor("picture"));
		desc.addFamily(new HColumnDescriptor("desc"));
		try {
			admin.createTable(desc, "20170825".getBytes(), "20180825".getBytes(), 20);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean upload(String filepath, byte[] content, int repeat) {

		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(ServiceConfig.tableName));
		} catch (IOException e) {
			logger.error("Failed getting the table");
			e.printStackTrace();
			return false;
		}

		String filename = null;
		int slashPos = filepath.lastIndexOf("/");
		if (slashPos >= 0) {
			filename = filepath.substring(slashPos + 1);
		} else {
			filename = filepath;
		}

		if (repeat <= 0) {
			logger.error("Repeat is invalid, using 1");
			repeat = 1;
		}

		byte[] size = Integer.toString(content.length).getBytes();
		byte[] status = "1".getBytes();
		byte[] timestamp = Long.toString(System.currentTimeMillis()).getBytes();

		List<Put> puts = new ArrayList<Put>();
		for (int index = 0; index < repeat; index++) {
			String row = String.format("%s/%s", digest.digest(filename.getBytes()), filename);
			Put p = new Put(row.getBytes());
			p.addColumn("picture".getBytes(), "picture".getBytes(), content);
			p.addColumn("desc".getBytes(), "size".getBytes(), size);
			p.addColumn("desc".getBytes(), "status".getBytes(), status);
			p.addColumn("desc".getBytes(), "timestamp".getBytes(), timestamp);
			puts.add(p);
		}

		try {
			table.put(puts);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}
	/**
	 * Title:多文件上传
	 * Author:TCE-MengEn.Cao
	 * Date：2017年10月26日 下午5:58:05
	 */
	public boolean upload(List<String> paths) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(ServiceConfig.tableName));
		} catch (IOException e) {
			logger.error("Failed getting the table");
			e.printStackTrace();
			return false;
		}

		List<Put> puts = new ArrayList<Put>();
		for (String path: paths) {

			String filename = null;
			//win linux 地址最后分隔符的位置
			int slashPos = path.lastIndexOf(os.indexOf("linux") >= 0 ? "/" : "\\");
			//
			if (slashPos >= 0) {
				//切割获取文件名
				filename = path.substring(slashPos + 1);
			} else {
				filename = path;
			}

			String[] tokens = filename.split("_");
			//按规则切割为6部分,不符合跳过继续
			//4_201708_18_14_09213213_1D67C5765280D45d9B396DC2A5C9CD036.jpg.ok
			if (tokens.length != 6) {
				logger.warn(String.format("Invalid filename: %s, skipping", filename));
				continue;
			}
			//文件名即存储Key
			String row = null;
			if (ServiceConfig.rowKeySlash) {
				row = String.format("/%s/%s/%s/%s/%s", tokens[1], tokens[2], tokens[3], tokens[4], tokens[5].replaceAll("\\.ok", ""));
			} else {
				row = String.format("%s/%s/%s/%s/%s", tokens[1], tokens[2], tokens[3], tokens[4], tokens[5].replaceAll("\\.ok", ""));
			}
//			logger.info(String.format("Row for %s: %s", filename, row));

			File file = new File(path);
			FileInputStream input = null;
			try {
				input = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				logger.error(String.format("No such file %s, skipping", path));
				continue;
			}

			byte[] content = null;
			try {
				content = IOUtils.toByteArray(input);
				input.close();
			} catch (IOException e) {
				logger.error(String.format("Failed getting bytes from %s", path));
				e.printStackTrace();
				continue;
			}
			//TODO 获取文件大小 
			byte[] size = Integer.toString(content.length).getBytes();
			byte[] status = "1".getBytes();
			byte[] timestamp = Long.toString(System.currentTimeMillis()).getBytes();

//			logger.info(String.format("Size of %s: %d", filename, content.length));
			Put p = new Put(row.getBytes());
			p.addColumn("picture".getBytes(), "picture".getBytes(), content);
			p.addColumn("desc".getBytes(), "size".getBytes(), size);
			p.addColumn("desc".getBytes(), "status".getBytes(), status);
			p.addColumn("desc".getBytes(), "timestamp".getBytes(), timestamp);
			
			//存入HBase集合
			puts.add(p);
		}
		//如果存入HBase文件的个数小于传过来的文件个数说明：有文件在存的过程中出错
		if (puts.size() < paths.size()) {
			logger.warn("Failed at some files of the batch");
			return false;
		}
		// 存入HBase
		try {
			table.put(puts);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}
	/**
	 * Title:单文件上传
	 * Author:TCE-MengEn.Cao
	 * Date：2017年10月26日 下午5:58:26
	 */
	public boolean upload(String filepath) {

		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(ServiceConfig.tableName));
		} catch (IOException e) {
			logger.error("Failed getting the table");
			e.printStackTrace();
			return false;
		}

		String filename = null;
		int slashPos = filepath.lastIndexOf("/");
		if (slashPos >= 0) {
			filename = filepath.substring(slashPos + 1);
		} else {
			filename = filepath;
		}

		// filename example: 4_201708_18_14_09213213_1D67C5765280D45d9B396DC2A5C9CD036.jpg.ok
		String[] tokens = filename.split("_");
		if (tokens.length != 6) {
			logger.error(String.format("Filename is invalid: %s", filename));
			return false;
		}
		String row = String.format("%s/%s/%s/%s/%s", tokens[1], tokens[2], tokens[3], tokens[4], tokens[5].replaceAll("\\.ok", ""));
		logger.info(String.format("Row key: %s", row));

		File file = new File(filepath);
		FileInputStream input = null;
		try {
			input = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			logger.error(String.format("No such file %s", filepath));
			e.printStackTrace();
			return false;
		}

		byte[] content = null;
		try {
			content = IOUtils.toByteArray(input);
		} catch (IOException e) {
			logger.error(String.format("Failed getting bytes from %s", filepath));
			e.printStackTrace();
			return false;
		}

		byte[] size = Integer.toString(content.length).getBytes();
		byte[] status = "1".getBytes();
		byte[] timestamp = Long.toString(System.currentTimeMillis()).getBytes();

		logger.info(String.format("Size: %d", content.length));
		logger.info("Try putting it to hbase");
		Put p = new Put(row.getBytes());
		p.addColumn("picture".getBytes(), "picture".getBytes(), content);
		p.addColumn("desc".getBytes(), "size".getBytes(), size);
		p.addColumn("desc".getBytes(), "status".getBytes(), status);
		p.addColumn("desc".getBytes(), "timestamp".getBytes(), timestamp);

		try {
			table.put(p);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	// e.g., reupload 20170707 13
	public void reupload(String date, String hour) {
//		logger.info(String.format("Start reuploading for hour: %s%s", date, hour));
//		File folder = new File(String.format("%s/%s/%s", ServiceConfig.photoPath, date, hour));
//		File[] files = folder.listFiles();
//		if (files == null) {
//			return;
//		}

//		for (File file: files) {
//			String filePath = file.getAbsolutePath();
//			for (String bucketName: ServiceConfig.bucketNames) {
//				if (upload(filePath, bucketName)) {
//					writeLog(filePath, bucketName, "0");
//				} else {
//					writeLog(filePath, bucketName, "1");
//				}
//			}
//		}
	}

	// e.g., reupload 20170707
	public void reupload(String date) {
//		logger.info(String.format("Start reuploading for day: %s", date));
//		for (int index = 0; index < 24; index++) {
//			File folder = new File(String.format("%s/%s/%02d", ServiceConfig.photoPath, date, index));
//			File[] files = folder.listFiles();
//			if (files == null) {
//				continue;
//			}
//
//			for (File file: files) {
//				String filePath = file.getAbsolutePath();
//				for (String bucketName: ServiceConfig.bucketNames) {
//					if (upload(filePath, bucketName)) {
//						writeLog(filePath, bucketName, "0");					
//					} else {
//						writeLog(filePath, bucketName, "1");
//					}
//				}
//			}
//		}
	}

	public void download(String bucketName, String fileName) {
//		for (Bucket bucket: buckets) {
//			if (bucket.getName().equals(bucketName)) {
//				logger.debug("Found " + bucketName);
//				OnestObject obj = onestClient.getObject(bucketName, fileName);
//				InputStream stream = obj.getObjectContent();
//				long size = obj.getObjectMetadata().getContentLength();
//				try {
//					OutputStream fileStream = new FileOutputStream(new File(fileName));
//					
//					IOUtils.copy(stream, fileStream);
//					fileStream.close();
//					logger.info(String.format("Downloaded %s from %s, size: %d", fileName, bucketName, (int)size));
//				} catch (IOException e) {
//					logger.error(String.format("Download error: %s", e.getMessage()));
//				}
//
//				break;
//			}
//		}
	}

	public void clear(String bucketName) {
		logger.info("Try clearing objects in " + bucketName);
//		for (Bucket bucket: buckets) {
//			if (bucket.getName().equals(bucketName)) {
//				logger.info("Found " + bucketName);
//				ObjectListing objs = onestClient.listObjects(bucketName);
//				for (OnestObjectSummary summary: objs.getObjectSummaries()) {
//					logger.info(String.format("Deleting object %s from bucket %s", summary.getKey(), bucketName));
//					onestClient.deleteObject(bucketName, summary.getKey());					
//				}
//			}
//		}
	}

	public void list(String bucketName) {
		logger.info("Try listing objects in " + bucketName);
//		for (Bucket bucket: buckets) {
//			logger.info("Found " + bucketName);
//			if (bucket.getName().equals(bucketName)) {
//				ObjectListing objs = onestClient.listObjects(bucketName);
//				for (OnestObjectSummary summary: objs.getObjectSummaries()) {
//					logger.info(String.format("Found object %s in bucket %s", summary.getKey(), bucketName));
//				}
//			}
//		}
	}
}
