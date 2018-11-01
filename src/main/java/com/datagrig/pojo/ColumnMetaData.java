package com.datagrig.pojo;

import java.sql.Types;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ColumnMetaData {
    private String name;
    private String schema;
    private String table;
    private String type;
    private String defaultValue;
    private int typeId;
    private int size;
    private boolean primaryKey;
    private boolean nullable;
    private boolean autoIncrement;
    private String comment;
    
    public boolean isBinary() {
    	return typeId == Types.BINARY 
    			|| typeId == Types.VARBINARY
    			|| typeId == Types.LONGVARBINARY;
    }

    public boolean isArray() {
    	return typeId == Types.ARRAY;
    }
}
