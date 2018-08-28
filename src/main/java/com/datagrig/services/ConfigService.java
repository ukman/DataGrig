package com.datagrig.services;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.tool.hbm2ddl.ForeignKeyMetadata;
import org.hibernate.tool.hbm2ddl.IndexMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class ConfigService {

    @Autowired
    private AppConfig appConfig;

    public List<File> getConnectionFolders() {
        return Arrays.stream(appConfig.getFolder().listFiles()).collect(Collectors.toList());
    }

    public ConnectionConfig getConnection(String connectionName) throws IOException {
        ObjectMapper mapper = createObjectMapper();
        File connectionConfigFile = getConnectionConfigFile(connectionName);
        ConnectionConfig connectionConfig = mapper.readValue(connectionConfigFile, ConnectionConfig.class);
        return connectionConfig;
    }

    public Optional<ForeignKeyMetaData[]> getCustomForeignKeys(String connectionName) throws IOException {
        ObjectMapper mapper = createObjectMapper();
        File connectionConfigFile = getCustomForeignKeysFile(connectionName);
        if(connectionConfigFile.exists()) {
            ForeignKeyMetaData[] foreignKeyMetaData = mapper.readValue(connectionConfigFile, ForeignKeyMetaData[].class);
            return Optional.of(foreignKeyMetaData);
        } else {
            return Optional.empty();
        }
    }

    protected ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    public File getConnectionFolder(String connectionName) {
        return new File(appConfig.getFolder(), connectionName);
    }

    public File getConnectionConfigFile(String connectionName) {
        File connectionFolder = getConnectionFolder(connectionName);
        return new File(connectionFolder, ConnectionConfig.FILE_NAME);
    }

    public File getCustomForeignKeysFile(String connectionName) {
        File connectionFolder = getConnectionFolder(connectionName);
        return new File(connectionFolder, ForeignKeyMetaData.FILE_NAME);
    }
}
