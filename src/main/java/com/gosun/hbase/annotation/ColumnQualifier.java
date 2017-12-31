package com.gosun.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 注解hbase，列限定符
 * @author cxp
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnQualifier {
	String value();
	String cf();
	String toBytesMethod() default "";
	String fromBytesMethod() default "";
}
