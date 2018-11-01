package com.datagrig.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class CatalogMetadata {
    private String name;
    private String dba;
    private String encoding;
    private String comment;
}
