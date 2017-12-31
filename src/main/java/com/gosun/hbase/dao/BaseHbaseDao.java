package com.gosun.hbase.dao;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.gosun.hbase.annotation.ColumnQualifier;
import com.gosun.hbase.annotation.HbaseTable;
import com.gosun.hbase.query.HbasePage;
import com.gosun.util.ReflectUtils;
import com.sun.tools.internal.xjc.model.SymbolSpace;

/**
 * Hbase数据通道抽象基类 T：实体类型，E：rowkey类型
 * 
 * @author cxp
 */
public abstract class BaseHbaseDao<T, E> {
	/**
	 * Hbase操作工具
	 */
	@Autowired
	protected HbaseTemplate hbaseTemplate;
	/**
	 * 泛型Class
	 */
	protected Class<T> tClass;
	/**
	 * Hbase表名
	 */
	protected String tableName;
	/**
	 * Hbase表的行键
	 */
	protected String rowKeyName;

	public BaseHbaseDao() {
		// 获取泛型Class
		if (tClass == null) {
			Type genType = getClass().getGenericSuperclass();
			Type[] params = ((ParameterizedType) genType)
					.getActualTypeArguments();
			tClass = (Class) params[0];
		}

		// 获取HbaseTable注解
		HbaseTable hbaseTable = (HbaseTable) tClass
				.getAnnotation(HbaseTable.class);
		assert hbaseTable != null
				&& StringUtils.isNotBlank(hbaseTable.rowKey())
				&& StringUtils.isNotBlank(hbaseTable.tableName());

		rowKeyName = hbaseTable.rowKey();
		tableName = hbaseTable.tableName();
	}
	
	public BaseHbaseDao(String tableName){
		this();
		this.tableName=tableName;
	}

	/**
	 * 查找所有记录
	 */
	public List<T> findAll() {
		// 全表扫描
		Scan scan = new Scan();
		return hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
			public T mapRow(Result result, int rowNum) throws Exception {
				return resultToBean(result);
			}
		});
	}

	/**
	 * 查找所有记录
	 */
	public List<T> findAll(String family) {
		return hbaseTemplate.find(tableName, family, new RowMapper<T>() {
			public T mapRow(Result result, int rowNum) throws Exception {
				return resultToBean(result);
			}
		});
	}

	/**
	 * 按rowkey获取对应记录
	 */
	public T getByRowKey(String rowName) {
		assert StringUtils.isNotBlank(rowName);

		return hbaseTemplate.get(tableName, rowName, new RowMapper<T>() {
			public T mapRow(Result result, int rowNum) throws Exception {
				return resultToBean(result);
			}
		});
	}

	/**
	 * 保存对象
	 */
	public void save(final T t) {
		hbaseTemplate.execute(tableName, new TableCallback<T>() {
			public T doInTable(HTableInterface table) throws Throwable {
				Put p = beanToPut(t);
				table.put(p);
				return t;
			}
		});
	}

	/**
	 * 保存所有对象
	 */
	public void saveAll(final List<T> tList) {
		assert tList != null;

		hbaseTemplate.execute(tableName, new TableCallback<T>() {
			public T doInTable(HTableInterface table) throws Throwable {

				List<Put> putList = new LinkedList<Put>();
				for (T t : tList) {
					Put p = beanToPut(t);
					putList.add(p);

				}
				table.put(putList);

				return null;
			}
		});
	}

	/**
	 * 分页查询，不推荐使用
	 */
	public HbasePage<T, E> list(HbasePage<T, E> page, FilterList filterList) {
		if (filterList == null) {
			filterList = new FilterList();
		}
		int pageSize = page.getPageSize();
		filterList.addFilter(new PageFilter(pageSize + 1));
		Scan scan = null;
		if (page.getStartRowKeyOfNextPage() != null) {
			scan = new Scan(objToBytes(page.getStartRowKeyOfNextPage()));
		} else {
			scan = new Scan();
		}
		scan.setFilter(filterList);

		List<T> beans = hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
			public T mapRow(Result result, int rowNum) throws Exception {
				return resultToBean(result);
			}
		});

		if (beans.size() < pageSize + 1) { // 最后一页
			page.setStartRowKeyOfNextPage(null);
		} else {
			T lastBean = beans.get(pageSize);
			E idOfLastBean = null;
			try {
				Field idField = tClass.getDeclaredField(rowKeyName);
				Method idGetter = ReflectUtils.obtainGetter(tClass, idField);
				idOfLastBean = (E) idGetter.invoke(lastBean);
			} catch (SecurityException e) {
				e.printStackTrace();// 1
			} catch (NoSuchFieldException e) {
				e.printStackTrace();// 1
			} catch (NoSuchMethodException e) {
				e.printStackTrace();// 2
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
			page.setStartRowKeyOfNextPage(idOfLastBean);
			beans.remove(pageSize + 1);
		}
		page.setBeans(beans);
		return page;
	}

	/**
	 * 统计总数
	 * @deprecated 查询速度非常慢
	 */
	@Deprecated
	public long countAll() {
		long result=0;
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		List<Integer> rows=hbaseTemplate.find(tableName, scan, new RowMapper<Integer>() {

			public Integer mapRow(Result result, int rowNum) throws Exception {
				return 1;
			}
		});
		
		result=rows.size();
		rows=hbaseTemplate.find(tableName, scan, new RowMapper<Integer>() {

			public Integer mapRow(Result result, int rowNum) throws Exception {
				return rowNum;
			}
		});
		
		System.out.println(rows);
		
		return result;
	}

	/**
	 * 将hbaseTemplate生成的result映射成实体实例
	 */
	protected T resultToBean(Result result) throws SecurityException,
			InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchFieldException, NoSuchMethodException {
		T t = tClass.newInstance();

		// 注入rowKey
		Field idField = tClass.getDeclaredField(rowKeyName);
		Method setIdMethod = ReflectUtils.obtainSetter(tClass, idField);
		Object id = bytesToObj(result.getRow(), idField.getType());
		if (id != null)
			setIdMethod.invoke(t, id);

		// 解析注解
		for (Field field : tClass.getDeclaredFields()) {
			ColumnQualifier cq = field.getAnnotation(ColumnQualifier.class);
			if (cq == null || StringUtils.isBlank(cq.value()))
				continue;
			String cqStr = cq.value();
			String cf = cq.cf();
			Method setMethod = ReflectUtils.obtainSetter(tClass, field);
			Object value = bytesToObj(
					result.getValue(Bytes.toBytes(cf), Bytes.toBytes(cqStr)),
					field.getType());
			if (value != null)
				setMethod.invoke(t, value);
		}
		return t;
	}

	/**
	 * 将实体实例映射成hbase.client.Put
	 */
	protected Put beanToPut(T t) throws NoSuchMethodException,
			SecurityException, NoSuchFieldException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		assert t != null;

		Field idField = tClass.getDeclaredField(rowKeyName);
		Method getIdMethod = ReflectUtils.obtainGetter(tClass, idField);
		Object id = getIdMethod.invoke(t);
		Put put = new Put(objToBytes(id));

		// 解析注解
		for (Field field : tClass.getDeclaredFields()) {
			ColumnQualifier cq = field.getAnnotation(ColumnQualifier.class);
			if (cq == null || StringUtils.isBlank(cq.value()))
				continue;
			String cqStr = cq.value();
			String cf = cq.cf();
			Method getMethod = ReflectUtils.obtainGetter(tClass, field);
			Object value = getMethod.invoke(t);
			put.add(Bytes.toBytes(cf), Bytes.toBytes(cqStr), objToBytes(value));
		}
		return put;
	}

	/**
	 * 将字节数组转换成object
	 * 只支持String，Integer，Long，Double，Float，Boolean，Short，BigDecimal
	 * ，ByteBuffer，Date
	 * 
	 * @param type
	 *            目标类型class
	 * @return maybe null
	 */
	protected Object bytesToObj(byte[] bytes, Class type) {
		Object value = null;
		if (bytes == null)
			return value;
		if (type.equals(Date.class)) {
			long time = Bytes.toLong(bytes);
			value = new Date(time);
		} else {
			String typeSName = type.getSimpleName();
			if (typeSName.equals("Integer")) {
				typeSName = "int";
			}
			try {
				Method bytesToMethod = Bytes.class.getMethod(
						"to" + StringUtils.capitalize(typeSName), byte[].class);
				value = bytesToMethod.invoke(new Bytes(), bytes);
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			}
		}
		return value;
	}

	/**
	 * 将对象转换成字节数组
	 * 
	 * @author cxp
	 *         只支持String，Integer，Long，Double，Float，Boolean，Short，BigDecimal，ByteBuffer
	 *         ，Date
	 * @return not null
	 */
	protected byte[] objToBytes(Object obj) {
		byte[] bytes = new byte[0];
		if (obj instanceof String) {
			bytes = Bytes.toBytes((String) obj);
		} else if (obj instanceof Integer) {
			bytes = Bytes.toBytes((Integer) obj);
		} else if (obj instanceof Long) {
			bytes = Bytes.toBytes((Long) obj);
		} else if (obj instanceof Double) {
			bytes = Bytes.toBytes((Double) obj);
		} else if (obj instanceof Float) {
			bytes = Bytes.toBytes((Float) obj);
		} else if (obj instanceof Boolean) {
			bytes = Bytes.toBytes((Boolean) obj);
		} else if (obj instanceof Short) {
			bytes = Bytes.toBytes((Short) obj);
		} else if (obj instanceof BigDecimal) {
			bytes = Bytes.toBytes((BigDecimal) obj);
		} else if (obj instanceof ByteBuffer) {
			bytes = Bytes.toBytes((ByteBuffer) obj);
		} else if (obj instanceof Date) {
			bytes = Bytes.toBytes(((Date) obj).getTime());
		}
		return bytes;
	}
}
