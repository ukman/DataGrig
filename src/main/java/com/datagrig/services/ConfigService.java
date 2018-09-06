package com.datagrig.services;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.pojo.TestConnectionResult;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.util.internal.ObjectUtil;

import org.apache.commons.lang3.ObjectUtils;
import org.hibernate.tool.hbm2ddl.ForeignKeyMetadata;
import org.hibernate.tool.hbm2ddl.IndexMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

	public ConnectionConfig saveConnection(String connectionName, ConnectionConfig connectionConfig) throws JsonGenerationException, JsonMappingException, IOException {
        File connectionFile = getConnectionConfigFile(connectionName);
        if(!connectionFile.getParentFile().exists()) {
        	connectionFile.getParentFile().mkdirs();
        }
        ObjectMapper objectMapper = createObjectMapper();
        objectMapper.writeValue(connectionFile, connectionConfig);
		return getConnection(connectionName);
	}

	public void deleteConnection(String connectionName) {
        File connectionFile = getConnectionFolder(connectionName);
        FileSystemUtils.deleteRecursively(connectionFile);
	}

	public TestConnectionResult testConnection(String connectionName, ConnectionConfig connectionConfig) throws ClassNotFoundException, SQLException {
		Class.forName(connectionConfig.getDriver());
		try(Connection con = DriverManager.getConnection(connectionConfig.getUrl(), connectionConfig.getUser(), connectionConfig.getPassword())) {
			try(Statement st = con.createStatement()) {
				st.executeQuery("select 1");
				if(ObjectUtils.firstNonNull(connectionConfig.getAliasQuery(), "").trim().length() > 0) {
					ResultSet rs = st.executeQuery(connectionConfig.getAliasQuery());
					rs.next();
					String alias = rs.getString(1);
					return TestConnectionResult.builder()
							.alias(alias)
							.build();
				}
			}
		}
		return TestConnectionResult.builder()
				.build();
	}
}
