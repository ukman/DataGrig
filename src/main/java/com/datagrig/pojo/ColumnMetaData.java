package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ColumnMetaData {
    private String name;
    private String type;
    private int size;
}
