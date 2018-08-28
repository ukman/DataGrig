package com.datagrig.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConnectionCatalog {
    private String connection;
    private String catalog;
}
