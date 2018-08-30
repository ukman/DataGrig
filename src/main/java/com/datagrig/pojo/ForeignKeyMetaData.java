package com.datagrig.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ForeignKeyMetaData {
    public static final String FILE_NAME = "custom-foreign-keys.json";
    private String name;
    private String detailsSchema;
    private String detailsTable;
    private String masterSchema;
    private String masterTable;
    private String pkFieldNameInMasterTable;
    private String fkFieldNameInDetailsTable;
    private String updateRule;
    private String deleteRule;
    private String linker;
}
