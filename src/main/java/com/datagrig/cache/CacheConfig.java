package com.datagrig.cache;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CacheConfig {
	public static final String DATA_SOURCES = "DATA_SOURCES";
	public static final String METADATA_CATALOGS = "METADATA_CATALOGS";
	public static final String METADATA_SCHEMAS = "METADATA_SCHEMAS";
	public static final String METADATA_TABLES = "METADATA_TABLES";
	public static final String METADATA_COLUMNS = "METADATA_COLUMNS";
	public static final String METADATA_FOREIGN_KEYS = "METADATA_FOREIGN_KEYS";
}
