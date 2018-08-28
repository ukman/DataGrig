package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ForeignKeyMetaData {
    public static final String FILE_NAME = "custom-foreign-keys.json";
    private String name;
    private String detailsTable;
    private String masterTable;
    private String pkFieldNameInMasterTable;
    private String fkFieldNameInDetailsTable;
    private String updateRule;
    private String deleteRule;
    private String linker;
}
