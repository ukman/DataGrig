package com.datagrig.pojo;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryInfo {
	private String query;
	private List<String> tableNames;
	private List<ForeignKeyMetaData> masterForeignKeys;
	private List<ForeignKeyMetaData> detailForeignKeys;

}
