package com.datagrig.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.datagrig.ssh.SSHKeepAlive;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.datagrig.ConnectionConfig;
import com.datagrig.cache.CacheConfig;
import com.datagrig.pojo.ConnectionUrl;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DataSourceService {

    @Autowired
    private SSHKeepAlive sshKeepAlive;

	@Autowired
	private ConfigService configService;
	
	// private Map<String, Pair<Session, Integer>> sshSessions = new HashMap<String, Pair<Session,Integer>>();;
	
	@Cacheable(CacheConfig.DATA_SOURCES)
    public DataSource getDataSource(String connectionName, String catalog) throws IOException, JSchException {
		log.info(String.format("Create datasource for %s.%s", connectionName, catalog));
        ConnectionConfig configConnection = configService.getConnection(connectionName);
        if(configConnection.isSsh()) {
            sshKeepAlive.addConfig(configConnection);
        }

        ConnectionUrl connectionUrl = new ConnectionUrl(configConnection.getUrl());
        if(configConnection.isSsh()) {
            int port = sshKeepAlive.getForwardedPort(configConnection);
            connectionUrl.setPort(port);
            connectionUrl.setHost("localhost");
        }

        if(catalog != null) {
	        connectionUrl.setCatalog(catalog);
        }
        String jdbcUrl = connectionUrl.toUrl();

        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName(configConnection.getDriver());
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(configConnection.getUser());
        ds.setPassword(configConnection.getPassword());
        return ds;
    }

    public boolean isPostgreDB(String connectionName) throws IOException {
    	ConnectionConfig connectionConfig = configService.getConnection(connectionName);
		return connectionConfig.getDriver().toLowerCase().contains("postgresql");
	}

    public boolean isMySQLDB(String connectionName) throws IOException {
    	ConnectionConfig connectionConfig = configService.getConnection(connectionName);
		return connectionConfig.getDriver().toLowerCase().contains("mysql");
	}

}
