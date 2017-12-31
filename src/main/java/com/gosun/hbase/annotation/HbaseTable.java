package com.gosun.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Hbase Table注解
 * @author cxp
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseTable {
	/**
	 * table名
	 * @return
	 */
	String tableName() default "";
	/**
	 * 行键
	 * @return
	 */
	String rowKey();
}
