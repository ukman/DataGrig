package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ColumnMetaData {
    private String name;
    private String type;
    private String defaultValue;
    private int typeId;
    private int size;
    private boolean primaryKey;
    private boolean nullable;
    private boolean autoIncrement;
    private String comment;
}
