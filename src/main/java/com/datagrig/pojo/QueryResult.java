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
}
