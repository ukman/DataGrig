package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class QueryResult {
    private List<ColumnMetaData> metaData;
    private List<Map<String, Object>> data;
    private int totalCount;
    private int page;
    private int lastPage;
    private int limit;
    
    public boolean hasNext() {
    	return page < lastPage - 1;
    }
    public boolean hasPrev() {
    	return page > 0;
    }
    public boolean hasLast() {
    	return lastPage > 1 && hasNext();
    }
    public boolean hasFirst() {
    	return page > 0 && hasPrev();
    }
    
}
