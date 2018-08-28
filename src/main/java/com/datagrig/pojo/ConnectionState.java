package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConnectionState {
    private String name;
    private boolean connected;
    private String databaseProductName;
    private String databaseProductVersion;
    private int databaseMinorVersion;
    private int databaseMajorVersion;
}
