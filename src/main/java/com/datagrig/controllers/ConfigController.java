package com.datagrig.controllers;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.pojo.TestConnectionResult;
import com.datagrig.services.ConfigService;
import com.jcraft.jsch.JSchException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
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

    @RequestMapping(path="/connections/{connectionName}", method = RequestMethod.GET)
    public ConnectionConfig getConnection(@PathVariable("connectionName") String connectionName) throws IOException {
        return configService.getConnection(connectionName);
    }

    @RequestMapping(path="/connections/{connectionName}", method = RequestMethod.POST)
    public ConnectionConfig saveConnection(@PathVariable("connectionName") String connectionName, @RequestBody ConnectionConfig connectionConfig) throws IOException {
        return configService.saveConnection(connectionName, connectionConfig);
    }

    @RequestMapping(path="/connections/{connectionName}/test", method = RequestMethod.PUT)
    public TestConnectionResult testConnection(@PathVariable("connectionName") String connectionName, @RequestBody ConnectionConfig connectionConfig) throws ClassNotFoundException, SQLException, JSchException {
        return configService.testConnection(connectionName, connectionConfig);
    }

    @RequestMapping(path="/connections/{connectionName}", method = RequestMethod.DELETE)
    public void deleteConnection(@PathVariable("connectionName") String connectionName) {
        configService.deleteConnection(connectionName);
    }

    @RequestMapping("/connections/{connectionName}/customForeignKeys")
    public ForeignKeyMetaData[] getCustomForeignKeys(@PathVariable("connectionName") String connectionName) throws IOException {
        Optional<ForeignKeyMetaData[]> metadata = configService.getCustomForeignKeys(connectionName);
        return  metadata.orElse(new ForeignKeyMetaData[0]);
    }
}

