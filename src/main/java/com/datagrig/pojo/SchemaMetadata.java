package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SchemaMetadata {
    private String name;
    private String comment;
}
