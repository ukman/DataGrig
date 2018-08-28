package com.datagrig;

import lombok.Data;

@Data
public class ConnectionConfig {
    public static final String FILE_NAME = "connection-config.json";

    private String driver;
    private String url;
    private String user;
    private String password;
    private String catalogs;

    /**
     * Alias query should return true/false for alias name as the only param.
     * E.g.
     * select exists(select * from options where db_name = ?)
     */
    private String aliasQuery;
}
