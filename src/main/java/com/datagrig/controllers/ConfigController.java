package com.datagrig.controllers;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.services.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/config")
public class ConfigController {

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private ConfigService configService;

    @RequestMapping("")
    public AppConfig getConfig() {
        return appConfig;
    }

    @RequestMapping("/connections")
    public List<String> getConnectionNames() {
        return configService.getConnectionFolders().stream().map(File::getName).collect(Collectors.toList());
    }

    @RequestMapping("/connections/{connectionName}")
    public ConnectionConfig getConnection(@PathVariable("connectionName") String connectionName) throws IOException {
        return configService.getConnection(connectionName);
    }

    @RequestMapping("/connections/{connectionName}/customForeignKeys")
    public ForeignKeyMetaData[] getCustomForeignKeys(@PathVariable("connectionName") String connectionName) throws IOException {
        Optional<ForeignKeyMetaData[]> metadata = configService.getCustomForeignKeys(connectionName);
        return  metadata.orElse(new ForeignKeyMetaData[0]);
    }
}

