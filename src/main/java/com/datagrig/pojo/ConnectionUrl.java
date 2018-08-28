package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class ConnectionUrl {
    private String protocol;
    private String type;
    private String host;
    private int port;
    private String catalog;

    public String toUrl() {
        return protocol + ":" + type + "://" + host + (port > 0 ? ":" + port : "") + "/" + catalog;
    }
}
