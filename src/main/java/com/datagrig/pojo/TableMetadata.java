package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TableMetadata {
    private String name;
    private String schema;
    private String type;
    private String comment;
    private long size;
}
