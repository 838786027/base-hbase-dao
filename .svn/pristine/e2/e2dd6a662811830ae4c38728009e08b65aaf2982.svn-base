package com.gosun.hbase.query;

import java.util.List;

/**
 * Hbase分页
 * @author caixiaopeng
 *
 * @param <T> bean类型
 * @param <E> rowkey类型
 */
public class HbasePage<T,E> {
	private Integer pageSize;
	private E startRowKeyOfNextPage;
	private List<T> beans;

	public Integer getPageSize() {
		return pageSize;
	}
	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}
	public E getStartRowKeyOfNextPage() {
		return startRowKeyOfNextPage;
	}
	public void setStartRowKeyOfNextPage(E startRowKeyOfNextPage) {
		this.startRowKeyOfNextPage = startRowKeyOfNextPage;
	}
	public List<T> getBeans() {
		return beans;
	}
	public void setBeans(List<T> beans) {
		this.beans = beans;
	}
}
