package com.datagrig.services;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.datagrig.pojo.*;
import com.google.common.base.CaseFormat;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.cache.CacheConfig;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcraft.jsch.JSchException;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MetadataService {

	private static final String DEFAULT_MYSQL_SCHEMA_NAME = "default";
	@Autowired
	private DataSourceService dataSourceService;
	
	@Autowired
	private ConfigService configService;
	
	@Autowired
	private AppConfig appConfig; 
	
	@Autowired
	private ApplicationContext applicationContext;
	
	public static final Set<Integer> TEXT_TYPES = new HashSet<Integer>(Arrays.asList(new Integer[]{
			Types.CHAR,
			Types.VARCHAR,
			Types.LONGNVARCHAR,
			Types.LONGVARCHAR,
			Types.NCHAR,
			Types.NVARCHAR,
	}));
	public static final Set<Integer> INT_TYPES = new HashSet<Integer>(Arrays.asList(new Integer[]{
			Types.BIGINT,
			Types.INTEGER,
			Types.TINYINT,
			Types.ROWID,
	}));

    public ConnectionState getConnection(String connectionName) throws IOException, JSchException {
        ConnectionState state = ConnectionState.builder()
                .name(connectionName)
                .connected(false)
                .build();

        DataSource ds = dataSourceService.getDataSource(connectionName, null);
    	try(Connection connection = ds.getConnection()) {
				DatabaseMetaData md = connection.getMetaData();
				state.setDatabaseProductName(md.getDatabaseProductName());
				state.setDatabaseProductVersion(md.getDatabaseProductVersion());
				state.setDatabaseMajorVersion(md.getDatabaseMajorVersion());
				state.setDatabaseMinorVersion(md.getDatabaseMinorVersion());
				state.setConnected(true);
		} catch (SQLException e) {
			log.error(String.format("Error getting driver name info for connection %s", state.getName()), e);
		}
		return state;
    }

    @Cacheable(CacheConfig.METADATA_CATALOGS)
    public List<CatalogMetadata> getConnectionCatalogs(String connectionName) throws SQLException, IOException, JSchException {
    	
        DataSource ds = dataSourceService.getDataSource(connectionName, null);
        ConnectionConfig configConnection = configService.getConnection(connectionName);
    	if(dataSourceService.isPostgreDB(connectionName)) {
	        JdbcTemplate template = new JdbcTemplate(ds);
	        List<String> excludeCatalogs = ObjectUtils.firstNonNull(configConnection.getExcludeCatalogs(), Collections.emptyList());
	        
	        RowMapper<CatalogMetadata> rowMapper = new RowMapper<CatalogMetadata>() {

				@Override
				public CatalogMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
					
					return CatalogMetadata.builder()
							.name(rs.getString("name"))
							//.dba(rs.getString("dba"))
							.encoding(rs.getString("encoding"))
							.build();
				}
			};
			String query = "SELECT db.datname as name, pg_encoding_to_char(db.encoding) as encoding "
	        		+ "FROM pg_database db "
	        		+ "WHERE db.datistemplate = false";

	        List<CatalogMetadata> catalogs = template.query(query, rowMapper);
	        catalogs = catalogs.stream().filter(cm -> !excludeCatalogs.contains(cm.getName())).collect(Collectors.toList());
	        catalogs.forEach(cm -> {
	        	try {
		        	String comment = template.queryForObject("SELECT description FROM pg_database LEFT JOIN pg_shdescription ON objoid = pg_database.oid WHERE datname = ?", String.class, cm.getName());
		        	cm.setComment(comment);
	        	} catch(DataAccessException e) {
	        		log.error(String.format("Error getting comment for catalog '%s'", cm.getName()), e);
	        	}
	        	try {
	        		String dba = template.queryForObject("SELECT a.rolname AS dba FROM pg_database db JOIN pg_authid a ON db.datdba = a.oid WHERE db.datname = ?", String.class, cm.getName());
	        		cm.setDba(dba);
	        	} catch(DataAccessException e) {
	        		log.error(String.format("Error getting DBA for catalog '%s'", cm.getName()), e);
	        	}
	        });        
	        
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
    public List<SchemaMetadata> getSchemas(String connectionName, String catalog) throws SQLException, IOException, JSchException {
    	if(dataSourceService.isPostgreDB(connectionName)) {
	        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
            List<SchemaMetadata> schemas = new ArrayList<>();
	        try (Connection connection = ds.getConnection()) {
	            DatabaseMetaData metadata = connection.getMetaData();
	            ResultSet schemasRs = metadata.getSchemas();
	            while(schemasRs.next()) {
	                schemas.add(SchemaMetadata.builder()
	                        .name(schemasRs.getString("TABLE_SCHEM"))
	                        .build());
	                
	            }
	        }
	        
	        JdbcTemplate template = new JdbcTemplate(ds);
	        schemas.forEach(sm -> {
	        	try {
	        		log.info("Schema " + sm.getName());
		        	String comment = template.queryForObject("SELECT obj_description(oid) FROM pg_catalog.pg_namespace schema WHERE nspname = ?", String.class, sm.getName());
		        	sm.setComment(comment);
	        	}catch(Exception e) {
	        		log.error("Error getting schema remarks", e);
	        	}
	        });
            return schemas;
    	}

    	if(dataSourceService.isMySQLDB(connectionName)) {
    		List<SchemaMetadata> schemas = Arrays.asList(SchemaMetadata.builder()
    				.name(DEFAULT_MYSQL_SCHEMA_NAME)
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
    public List<TableMetadata> getTables(String connectionName, String catalog) throws SQLException, IOException, JSchException {

        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet tablesRs = metadata.getTables(catalog, null, null, null);
            List<TableMetadata> tables = new ArrayList<>();
            Map<String, TableMetadata> mapTables = new HashMap<>();
            while (tablesRs.next()) {
                String type = tablesRs.getString("TABLE_TYPE");

                if (type != null && (type.toUpperCase().contains("TABLE") || type.toUpperCase().contains("VIEW"))) {
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
    public List<ForeignKeyMetaData> getExportedForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		
        List<ForeignKeyMetaData> indexes = null;
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            indexes = new ArrayList<>();
            ResultSet indexRs = metadata.getExportedKeys(catalog, schema, table);

            while (indexRs.next()) {
            	ForeignKeyMetaData index = ForeignKeyMetaData.builder()
                    .name(indexRs.getString("FK_NAME"))
                    .masterTable(indexRs.getString("PKTABLE_NAME"))
                    .masterSchema(indexRs.getString("PKTABLE_SCHEM"))
					.pkFieldNameInMasterTable(indexRs.getString("PKCOLUMN_NAME"))
					.detailsTable(indexRs.getString("FKTABLE_NAME"))
                    .detailsSchema(indexRs.getString("FKTABLE_SCHEM"))
                    .fkFieldNameInDetailsTable(indexRs.getString("FKCOLUMN_NAME"))
                    .updateRule(indexRs.getString("UPDATE_RULE"))
                    .deleteRule(indexRs.getString("DELETE_RULE"))
                    .build();
            	processCommentForFk(connectionName, catalog, index);
            	
                indexes.add(index);
            }

            // Process aliases
			processAliases(connectionName, indexes);

        }
        stopWatch.stop();
        log.info("Execution time of getMasterForeignKeysForAllTables = " + stopWatch.getTotalTimeSeconds() + " sec.");
        return indexes;
    }

    // Check if all indexes has aliases and set if it's null
	protected void processAliases(String connectionName, List<ForeignKeyMetaData> indexes) throws IOException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
		indexes.forEach(index -> {
			if(index.getAliasInDetailsTable() == null) {
				String alias = index.getFkFieldNameInDetailsTable();
				if(alias.toLowerCase().endsWith("_id")) {
					alias = alias.substring(0, alias.length() - 3);
				}
				alias = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, alias);
				index.setAliasInDetailsTable(alias);
			}
			if(index.getAliasInMasterTable() == null) {
				// No custom alias, so trying to use either table name or index name as alias...

				// ... check if there are more than 1 link between tables.
				if(indexes.stream().anyMatch(index2 -> {
					return index2 != index
							&& (isMysql || index.getDetailsSchema().equalsIgnoreCase(index2.getDetailsSchema()))
							&& index.getDetailsTable().equalsIgnoreCase(index2.getDetailsTable())
							&& (isMysql || index.getMasterSchema().equalsIgnoreCase(index2.getMasterSchema()))
							&& index.getMasterTable().equalsIgnoreCase(index2.getMasterTable());
				})) {
					// There are more than 1 link to this table- so we cannot use table name as connection alias.
					String alias = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, index.getName());
					alias = multi(alias);
					index.setAliasInMasterTable(alias);
				} else {
					// There is only 1 link betwee tables so we can use table name as unique link name
					String alias = (Objects.equals(index.getMasterSchema(), index.getDetailsSchema()) ? "" :  index.getDetailsSchema() + "_") + index.getDetailsTable();
					alias = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, alias);
					alias = multi(alias);
					index.setAliasInMasterTable(alias);
				}
			}
		});
	}

	protected String multi(String s) {
    	if(s.endsWith("s") || s.endsWith("z")) {
    		return s + "es";
		}
    	if(s.endsWith("y")) {
    		return s.substring(0 , s.length() - 1) + "ies";
		}
		return s + "s";
	}

	protected void processCommentForFk(String connectionName, String catalog, ForeignKeyMetaData index) throws SQLException, IOException, JSchException {
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
	
	

    public List<TableMetadata> getTables(String connectionName, String catalog, String schema) throws SQLException, IOException, JSchException {
    	List<TableMetadata> allTables = getSpringProxy().getTables(connectionName, catalog);
    	boolean isMySQL = dataSourceService.isMySQLDB(connectionName);
    	List<TableMetadata> tables = isMySQL ? allTables : allTables.stream().filter(t -> (t.getSchema() == null && schema == null) || (t.getSchema() != null && t.getSchema().equals(schema))).collect(Collectors.toList());
        return tables;
    }

    @Cacheable(CacheConfig.METADATA_COLUMNS)
    public List<ColumnMetaData> getColumnsCached(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        List<ColumnMetaData> columns = new ArrayList<>();
		boolean isMySql = dataSourceService.isMySQLDB(connectionName);

        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();

            try (ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null)) {
	            while (columnsRs.next()) {
	            	ColumnMetaData col = createColumnMetadata(columnsRs);
					if(isMySql) {
						col.setSchema(DEFAULT_MYSQL_SCHEMA_NAME);
					}
	                columns.add(col);
	            }
            }
            try (ResultSet pkRs = metadata.getPrimaryKeys(catalog, schema, table)) {
	            while(pkRs.next()) {
	                String colName = pkRs.getString("COLUMN_NAME");
	                String schemaName = pkRs.getString("TABLE_SCHEM");
	                String tableName = pkRs.getString("TABLE_NAME");
	                columns.stream()
							.filter(c -> {
								boolean schemaEq = isMySql || schemaName.equals(c.getSchema());
								return c.getName().equals(colName) &&
										c.getTable().equals(tableName) &&
										schemaEq;

							})
							.forEach(c -> c.setPrimaryKey(true));
	            }
            }
        }
        return columns;
    }
    
    public List<ColumnMetaData> getColumns(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
    	List<ColumnMetaData> allColumns =
				supportsNullTableForMetedata(connectionName) ?
				getSpringProxy().getColumnsCached(connectionName, catalog, null, null) :
				getSpringProxy().getColumnsCached(connectionName, catalog, schema, table);
    	List<ColumnMetaData> columns = allColumns.stream().filter(t -> t.getTable().equals(table) && (isMysql || t.getSchema().equals(schema))).collect(Collectors.toList());
    	return columns;
    }
    
    public ColumnMetaData getColumn(String connectionName, String catalog, String schema, String table, String columnName) throws SQLException, IOException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
		List<ColumnMetaData> allColumns = getSpringProxy().getColumns(connectionName, catalog, schema, table);
    	List<ColumnMetaData> columns = allColumns.stream().filter(t -> t.getName().equals(columnName) && t.getTable().equals(table) && (isMysql || t.getSchema().equals(schema))).collect(Collectors.toList());
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
		String schema = columnsRs.getString("TABLE_SCHEM");
    	return ColumnMetaData.builder()
                .name(columnsRs.getString("COLUMN_NAME"))
                .table(columnsRs.getString("TABLE_NAME"))
                .schema(schema)
                .type(columnsRs.getString("TYPE_NAME"))
                .typeId(columnsRs.getInt("DATA_TYPE"))
                .size(columnsRs.getInt("COLUMN_SIZE"))
                .comment(columnsRs.getString("REMARKS"))
                .nullable(isNullable)
                .autoIncrement(isAutoIncrement)
                .defaultValue(columnsRs.getString("COLUMN_DEF"))
                .build();
    }

	public String getIdentifierQuoteString(String connectionName, String catalog) throws SQLException, IOException, JSchException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            return metadata.getIdentifierQuoteString();
        }
    }

	public List<ForeignKeyMetaData> getForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet indexRs = metadata.getImportedKeys(catalog, schema, table);
            List<ForeignKeyMetaData> indexes = new ArrayList<>();
            while (indexRs.next()) {
            	ForeignKeyMetaData index = createIndexMetaData(connectionName, catalog, indexRs);
                indexes.add(index);
            }
            processAliases(connectionName, indexes);
            return indexes;
        }
    }

    private ForeignKeyMetaData createIndexMetaData(String connectionName, String catalog, ResultSet indexRs) throws SQLException, IOException, JSchException {
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

	public List<ForeignKeyMetaData> getMasterForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
        List<ForeignKeyMetaData> indexes =
				supportsNullTableForMetedata(connectionName) ?
						getSpringProxy().getExportedForeignKeys(connectionName, catalog, null, null ) :
						getSpringProxy().getExportedForeignKeys(connectionName, catalog, schema, table);
        return indexes.stream().filter(m -> table.equals(m.getMasterTable()) && (isMysql || schema.equals(m.getMasterSchema()))).collect(Collectors.toList());
    }
	
	public List<ForeignKeyMetaData> getDetailForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
        List<ForeignKeyMetaData> indexes =
				supportsNullTableForMetedata(connectionName) ?
//				getSpringProxy().getExportedForeignKeys(connectionName, catalog, null, null) :
//				getSpringProxy().getExportedForeignKeys(connectionName, catalog, schema, table);
				getSpringProxy().getForeignKeys(connectionName, catalog, null, null) :
				getSpringProxy().getForeignKeys(connectionName, catalog, schema, table);
		List<ForeignKeyMetaData> res = indexes.stream().filter(m -> table.equals(m.getDetailsTable()) && (isMysql || schema.equals(m.getDetailsSchema()))).collect(Collectors.toList());
		return res;
    }

    protected boolean supportsNullTableForMetedata(String connectionName) {
		try {
			return dataSourceService.isPostgreDB(connectionName);
		} catch (IOException e) {
			log.error("Error identifying supportsNullTableForMetedata", e);
		}
		return false;
	}

	private MetadataService getSpringProxy() {
	    return applicationContext.getBean(MetadataService.class);
	}

	protected ObjectMapper createObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		// objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return objectMapper;
	}
	
	@Cacheable(CacheConfig.METADATA_TITLE_COLUMN)
	public ColumnMetaData getLabelColumn(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		List<ColumnMetaData> columns = getSpringProxy().getColumns(connectionName, catalog, schema, table);
		Map<String, ColumnMetaData> columnMap = columns.stream().collect(Collectors.toMap(c -> {return c.getName().toLowerCase();}, c -> c));
		
		String[] labelColumns = appConfig.getLabelColumnNames();
		if(labelColumns != null) {
			for(String labelColumn : labelColumns) {
				if(columnMap.containsKey(labelColumn.toLowerCase())) {
					return columnMap.get(labelColumn.toLowerCase());
				}
			}
			for(String labelColumn : labelColumns) {
				for(ColumnMetaData col : columns) {
					if(col.getName().toLowerCase().contains(labelColumn)) {
						return col;
					}
				}
			}
		}
		return columns.stream().filter(c -> TEXT_TYPES.contains(c.getTypeId())).findFirst().orElse(columns.get(0));
	}

	@Cacheable(CacheConfig.METADATA_PK_COLUMN)
	public ColumnMetaData getPkColumn(String connectionName, String catalog, String schema, String table) throws SQLException, IOException, JSchException {
		List<ColumnMetaData> columns = getSpringProxy().getColumns(connectionName, catalog, schema, table);
		List<ColumnMetaData> pks = columns.stream().filter(c -> c.isPrimaryKey()).collect(Collectors.toList());
		if(pks.size() != 1) {
			throw new IllegalStateException(String.format("Table %s contains %d pk(s)", table, pks.size()));
		}
		return pks.get(0);
	}

	@Cacheable(CacheConfig.METADATA_SEQUENCES)
	public List<SequenceMetaData> getSequences(String connectionName, String catalog, String schema) throws IOException, JSchException {
		DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
		JdbcTemplate template = new JdbcTemplate(ds);

		RowMapper<SequenceMetaData> rowMapper = new RowMapper<SequenceMetaData>() {

			@Override
			public SequenceMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {
				return SequenceMetaData.builder()
						.name(rs.getString("sequence_name"))
						.increment(rs.getInt("increment"))
						.minValue(rs.getLong("minimum_value"))
						.maxValue(rs.getLong("maximum_value"))
						.build();
			}
		};
		List<SequenceMetaData> sequences = template.query("SELECT * FROM INFORMATION_SCHEMA.sequences Where sequence_catalog=? and sequence_schema=?", new Object[]{catalog, schema}, rowMapper);

// 		List<String> seqNames = template.queryForList("SELECT * FROM INFORMATION_SCHEMA.sequences Where sequence_catalog=? and sequence_schema=?", new Object[]{catalog, schema}, String.class);
		String select = sequences.stream().map(seq -> seq.getName() + ".last_value as " + seq.getName() + "_lv").collect(Collectors.joining(", "));
		String from = sequences.stream().map(SequenceMetaData::getName).collect(Collectors.joining(", "));

		Map<String, Object> lvs = template.queryForMap("select " + select + " from " + from);
		sequences.stream().forEach(seq -> {
			Long v = (Long) lvs.get(seq.getName() + "_lv");
			seq.setValue(v);
		});
		return sequences;
	}

}
