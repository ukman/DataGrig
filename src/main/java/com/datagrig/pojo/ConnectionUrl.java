package com.datagrig.pojo;

import org.apache.commons.lang3.ObjectUtils;

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
    private String params;

    public String toUrl() {
        return protocol + ":" + type + "://" + host + (port > 0 ? ":" + port : "") + "/" + catalog + ObjectUtils.firstNonNull(params, "");
    }
}
