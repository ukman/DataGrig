package com.datagrig.services;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.datagrig.ConnectionConfig;
import com.datagrig.cache.CacheConfig;
import com.datagrig.pojo.CatalogMetadata;
import com.datagrig.pojo.ColumnMetaData;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.pojo.SchemaMetadata;
import com.datagrig.pojo.TableMetadata;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MetadataService {
	
	@Autowired
	private DataSourceService dataSourceService;
	
	@Autowired
	private ConfigService configService;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Cacheable(CacheConfig.METADATA_CATALOGS)
    public List<CatalogMetadata> getConnectionCatalogs(String connectionName) throws SQLException, IOException {
    	
        DataSource ds = dataSourceService.getDataSource(connectionName, null);
        ConnectionConfig configConnection = configService.getConnection(connectionName);
    	if(dataSourceService.isPostgreDB(connectionName)) {
	        JdbcTemplate template = new JdbcTemplate(ds);
	        List<String> excludeCatalogs = ObjectUtils.firstNonNull(configConnection.getExcludeCatalogs(), Collections.emptyList());
	        
	        List<String> catalogNames = template.queryForList("SELECT datname as name FROM pg_database WHERE datistemplate = false", String.class);
	
	        List<CatalogMetadata> catalogs = catalogNames.stream()
	        		.filter(catalog -> !excludeCatalogs.contains(catalog))
	        		.map(CatalogMetadata::new).collect(Collectors.toList());
	        return catalogs;
    	}
    	
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet catalogsRs = metadata.getCatalogs();
            List<CatalogMetadata> catalogs = new ArrayList<>();
            while (catalogsRs.next()) {
                catalogs.add(CatalogMetadata.builder().name(catalogsRs.getString("TABLE_CAT")).build());
            }
            return catalogs;
        }
    }
	
	@Cacheable(CacheConfig.METADATA_SCHEMAS)
    public List<SchemaMetadata> getSchemas(String connectionName, String catalog) throws SQLException, IOException {
    	if(dataSourceService.isPostgreDB(connectionName)) {
	        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
	        try (Connection connection = ds.getConnection()) {
	            DatabaseMetaData metadata = connection.getMetaData();
	            ResultSet schemasRs = metadata.getSchemas();
	            List<SchemaMetadata> schemas = new ArrayList<>();
	            while(schemasRs.next()) {
	                schemas.add(SchemaMetadata.builder()
	                        .name(schemasRs.getString("TABLE_SCHEM"))
	                        .title(schemasRs.getString("TABLE_SCHEM"))
	                        .build());
	            }
	            return schemas;
	        }
    	}

    	if(dataSourceService.isMySQLDB(connectionName)) {
    		List<SchemaMetadata> schemas = Arrays.asList(SchemaMetadata.builder()
    				.title("default")
    				.name("default")
    				.build());
    		return schemas;
    	}

        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet schemaRs = metadata.getSchemas();
            List<SchemaMetadata> schemas = new ArrayList<>();
            while (schemaRs.next()) {
                schemas.add(SchemaMetadata.builder().name(schemaRs.getString("TABLE_SCHEM")).build());
            }
            return schemas;
        }
    }
	
	@Cacheable(CacheConfig.METADATA_TABLES)
    public List<TableMetadata> getTables(String connectionName, String catalog) throws SQLException, IOException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet tablesRs = metadata.getTables(catalog, null, null, null);
            List<TableMetadata> tables = new ArrayList<>();
            Map<String, TableMetadata> mapTables = new HashMap<>();
            while (tablesRs.next()) {
                String type = tablesRs.getString("TABLE_TYPE");

                if ("TABLE".equals(type)) {
                    TableMetadata table = TableMetadata.builder()
                            .name(tablesRs.getString("TABLE_NAME"))
                            .schema(tablesRs.getString("TABLE_SCHEM"))
                            .comment(tablesRs.getString("REMARKS"))
                            .type(type)
                            .build();
                    tables.add(table);
                    mapTables.put(table.getName(), table);
                }
            }

            if(dataSourceService.isPostgreDB(connectionName)) {
	            PreparedStatement ps = connection.prepareStatement("SELECT\n" +
	                    "   relname AS \"Table\",\n" +
	                    "   pg_total_relation_size(relid) AS \"RealSize\",\n" +
	                    "   pg_size_pretty(pg_total_relation_size(relid)) AS \"Size\",\n" +
	                    "   pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS \"External Size\"\n" +
	                    "   FROM pg_catalog.pg_statio_user_tables ORDER BY pg_total_relation_size(relid) DESC;");
	            ResultSet rs = ps.executeQuery();
	            while (rs.next()) {
	                String tableName = rs.getString("Table");
	                TableMetadata table = mapTables.get(tableName);
	                if (table != null) {
	                    table.setSize(rs.getLong("RealSize"));
	                }
	            }
            }
            return tables;
        }
    }

	@Cacheable(CacheConfig.METADATA_FOREIGN_KEYS)
    public List<ForeignKeyMetaData> getImportedForeignKeys(String connectionName, String catalog) throws SQLException, IOException {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
        List<ForeignKeyMetaData> indexes = null;
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            indexes = new ArrayList<>();
            ResultSet indexRs = metadata.getImportedKeys(catalog, null, null);

            while (indexRs.next()) {
            	ForeignKeyMetaData index = ForeignKeyMetaData.builder()
                    .name(indexRs.getString("FK_NAME"))
                    .masterTable(indexRs.getString("PKTABLE_NAME"))
                    .masterSchema(indexRs.getString("PKTABLE_SCHEM"))
                    .detailsTable(indexRs.getString("FKTABLE_NAME"))
                    .detailsSchema(indexRs.getString("FKTABLE_SCHEM"))
                    .pkFieldNameInMasterTable(indexRs.getString("PKCOLUMN_NAME"))
                    .fkFieldNameInDetailsTable(indexRs.getString("FKCOLUMN_NAME"))
                    .updateRule(indexRs.getString("UPDATE_RULE"))
                    .deleteRule(indexRs.getString("DELETE_RULE"))
                    .build();
            	processCommentForFk(connectionName, catalog, index);
            	
                indexes.add(index);
            }
        }
        stopWatch.stop();
        log.info("Execution time of getMasterForeignKeysForAllTables = " +stopWatch.getTotalTimeSeconds() + " sec.");
        return indexes;
    }

	protected void processCommentForFk(String connectionName, String catalog, ForeignKeyMetaData index) throws SQLException, IOException {
    	ColumnMetaData fkField = getColumn(connectionName, catalog, index.getDetailsSchema(), index.getDetailsTable(), index.getFkFieldNameInDetailsTable());
    	if(fkField.getComment() != null) {
    		ObjectMapper mapper = createObjectMapper();
    		try {
    			ForeignKeyMetaData fkMD = mapper.readValue(fkField.getComment(), ForeignKeyMetaData.class);
    			index.setAliasInDetailsTable(fkMD.getAliasInDetailsTable());
    			index.setAliasInMasterTable(fkMD.getAliasInMasterTable());
    		} catch (JsonParseException | JsonMappingException e) {
    			// Not problem- just skip comment
    			log.warn(String.format("Error parsing comment %s", fkField.getComment()), e);
			}
    	}
	}
	
	

    public List<TableMetadata> getTables(String connectionName, String catalog, String schema) throws SQLException, IOException {
    	List<TableMetadata> allTables = getSpringProxy().getTables(connectionName, catalog);
    	boolean isMySQL = dataSourceService.isMySQLDB(connectionName);
    	List<TableMetadata> tables = isMySQL ? allTables : allTables.stream().filter(t -> (t.getSchema() == null && schema == null) || (t.getSchema() != null && t.getSchema().equals(schema))).collect(Collectors.toList());
        return tables;
    }

    @Cacheable(CacheConfig.METADATA_COLUMNS)
    public List<ColumnMetaData> getColumns(String connectionName, String catalog) throws SQLException, IOException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        List<ColumnMetaData> columns = new ArrayList<>();
        
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet columnsRs = metadata.getColumns(catalog, null, null, null)) {
	            while (columnsRs.next()) {
	            	ColumnMetaData col = createColumnMetadata(columnsRs);
	                columns.add(col);
	            }
            }
            try (ResultSet pkRs = metadata.getPrimaryKeys(catalog, null, null)) {
	            while(pkRs.next()) {
	                String colName = pkRs.getString("COLUMN_NAME");
	                String schema = pkRs.getString("TABLE_SCHEM");
	                String table = pkRs.getString("TABLE_NAME");
	                columns.stream().filter(c ->c.getName().equals(colName) && c.getTable().equals(table) && c.getSchema().equals(schema)).forEach(c -> c.setPrimaryKey(true));
	            }
            }
        }
        return columns;
    }
    
    public List<ColumnMetaData> getColumns(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
    	List<ColumnMetaData> columns = getSpringProxy().getColumns(connectionName, catalog).stream().filter(t -> t.getTable().equals(table) && t.getSchema().equals(schema)).collect(Collectors.toList());
    	return columns;
    }
    
    public ColumnMetaData getColumn(String connectionName, String catalog, String schema, String table, String columnName) throws SQLException, IOException {
    	List<ColumnMetaData> columns = getSpringProxy().getColumns(connectionName, catalog).stream().filter(t -> t.getName().equals(columnName) && t.getTable().equals(table) && t.getSchema().equals(schema)).collect(Collectors.toList());
    	if(columns.size() == 1) {
    		return columns.get(0);
    	}
    	throw new IllegalArgumentException(String.format("Can not find column %s in table %s", columnName, table));
    }
    
    protected ColumnMetaData createColumnMetadata(ResultSet columnsRs) throws SQLException {
		String isNullableStr = columnsRs.getString("IS_NULLABLE");
		boolean isNullable = "YES".equalsIgnoreCase(isNullableStr);
		String isAutoIncrementStr = columnsRs.getString("IS_AUTOINCREMENT");
		boolean isAutoIncrement = "YES".equalsIgnoreCase(isAutoIncrementStr);
    	return ColumnMetaData.builder()
                .name(columnsRs.getString("COLUMN_NAME"))
                .table(columnsRs.getString("TABLE_NAME"))
                .schema(columnsRs.getString("TABLE_SCHEM"))
                .type(columnsRs.getString("TYPE_NAME"))
                .typeId(columnsRs.getInt("DATA_TYPE"))
                .size(columnsRs.getInt("COLUMN_SIZE"))
                .comment(columnsRs.getString("REMARKS"))
                .nullable(isNullable)
                .autoIncrement(isAutoIncrement)
                .defaultValue(columnsRs.getString("COLUMN_DEF"))
                .build();
    }

	public List<ForeignKeyMetaData> getForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet indexRs = metadata.getImportedKeys(catalog, schema, table);
            List<ForeignKeyMetaData> indexes = new ArrayList<>();
            while (indexRs.next()) {
            	ForeignKeyMetaData index = createIndexMetaData(connectionName, catalog, indexRs);
                indexes.add(index);
            }
            return indexes;
        }
    }

    private ForeignKeyMetaData createIndexMetaData(String connectionName, String catalog, ResultSet indexRs) throws SQLException, IOException {
    	ForeignKeyMetaData index = ForeignKeyMetaData.builder()
	        .name(indexRs.getString("FK_NAME"))
	        .masterTable(indexRs.getString("PKTABLE_NAME"))
	        .masterSchema(indexRs.getString("PKTABLE_SCHEM"))
	        .detailsTable(indexRs.getString("FKTABLE_NAME"))
	        .detailsSchema(indexRs.getString("FKTABLE_SCHEM"))
	        .pkFieldNameInMasterTable(indexRs.getString("PKCOLUMN_NAME"))
	        .fkFieldNameInDetailsTable(indexRs.getString("FKCOLUMN_NAME"))
	        .updateRule(indexRs.getString("UPDATE_RULE"))
	        .deleteRule(indexRs.getString("DELETE_RULE"))
	        .build();
    	processCommentForFk(connectionName, catalog, index);
        return index;
	}

	public List<ForeignKeyMetaData> getMasterForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        List<ForeignKeyMetaData> indexes = getSpringProxy().getImportedForeignKeys(connectionName, catalog);
        return indexes.stream().filter(m -> table.equals(m.getMasterTable()) && schema.equals(m.getMasterSchema())).collect(Collectors.toList());
    }
	
	public List<ForeignKeyMetaData> getDetailForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        List<ForeignKeyMetaData> indexes = getSpringProxy().getImportedForeignKeys(connectionName, catalog);
        return indexes.stream().filter(m -> table.equals(m.getDetailsTable()) && schema.equals(m.getDetailsSchema())).collect(Collectors.toList());
    }

	private MetadataService getSpringProxy() {
	    return applicationContext.getBean(MetadataService.class);
	}

	protected ObjectMapper createObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		// objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return objectMapper;
	}
}
