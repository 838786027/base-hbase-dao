package com.gosun.hbase.util;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于设置hadoop.home
 * 用于在windows下连接hbase
 * 
 * @author cxp
 *
 */
public class LoadHadoopHome {

	private final static Logger LOGGER=LoggerFactory.getLogger(LoadHadoopHome.class);
	
	/**
	 * 设置hadoop.home
	 */
	public static void load(){
		//用于在windows下连接hbase
		File directory = new File(".");
		String hadoopHome;
		try {
			hadoopHome = directory.getCanonicalPath()+"\\hadoopHome";
			System.setProperty("hadoop.home.dir", hadoopHome.replace("\\","/"));
		} catch (IOException e) {
			LOGGER.warn("加载hadoopHome异常",e);
		}
	}
}
