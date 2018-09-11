package com.datagrig.services;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.datagrig.ConnectionConfig;
import com.datagrig.cache.CacheConfig;
import com.datagrig.pojo.ConnectionUrl;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DataSourceService {
    private static final String CONNECTION_RE = "([^:]+):([^:]*)://([^:/]+)(:[0-9]+)?/([^?]*)(\\?.+)?"; //jdbc:postgresql://localhost:5432/cyfoman
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(CONNECTION_RE);

	@Autowired
	private ConfigService configService;
	
	@Cacheable(CacheConfig.DATA_SOURCES)
    public DataSource getDataSource(String connectionName, String catalog) throws IOException {
		log.info(String.format("Create datasource for %s.%s", connectionName, catalog));
        ConnectionConfig configConnection = configService.getConnection(connectionName);

        String jdbcUrl;
        if(catalog != null) {
	        ConnectionUrl connectionUrl = parseConnectionString(configConnection.getUrl());
	        connectionUrl.setCatalog(catalog);
	        jdbcUrl = connectionUrl.toUrl();
        } else {
        	jdbcUrl = configConnection.getUrl();
        }

        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName(configConnection.getDriver());
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(configConnection.getUser());
        ds.setPassword(configConnection.getPassword());
        return ds;
    }

    public ConnectionUrl parseConnectionString(String url) {
        Matcher m = CONNECTION_PATTERN.matcher(url);
        if(m.matches()) {
            String sPort = m.group(4);
            return ConnectionUrl.builder()
                    .protocol(m.group(1))
                    .type(m.group(2))
                    .host(m.group(3))
                    .port(sPort == null || sPort.length() == 0 ? 0 : Integer.parseInt(sPort.substring(1)))
                    .catalog(m.group(5))
                    .params(m.group(6))
                    .build();
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse url '?'", url));
        }
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
