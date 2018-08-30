package com.datagrig.services;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.CursorNode;
import com.akiban.sql.parser.FromList;
import com.akiban.sql.parser.FromTable;
import com.akiban.sql.parser.OrderByList;
import com.akiban.sql.parser.ResultSetNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SelectNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.TableName;
import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.*;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ConnectionService {

    private static final String CONNECTION_RE = "([^:]+):([^:]*)://([^:/]+)(:[0-9]+)?/(.*)"; //jdbc:postgresql://localhost:5432/cyfoman
    private static final Pattern CONNECTION_PATTERN = Pattern.compile(CONNECTION_RE);

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private ConfigService configService;

    private Map<String, HikariDataSource> connections = new HashMap<>();

    private Map<String, List<ForeignKeyMetaData>> indexesCache = new HashMap<>();

    public void connect(String connectionName, String catalog) throws IOException, ClassNotFoundException, SQLException {
        String key = connectionName + "/" + catalog;
        if(connections.containsKey(key)) {
            throw new IllegalStateException(String.format("Connection %s is already opened", key));
        }
        ConnectionConfig configConnection = configService.getConnection(connectionName);

        ConnectionUrl connectionUrl = parseConnectionString(configConnection.getUrl());
        connectionUrl.setCatalog(catalog);
        String jdbcUrl = connectionUrl.toUrl();

        HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName(configConnection.getDriver());
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(configConnection.getUser());
        ds.setPassword(configConnection.getPassword());
        connections.put(key, ds);
    }

    public void disconnect(String connectionName, String catalog) throws IOException, ClassNotFoundException, SQLException {
        connections.remove(connectionName + "/" + catalog);
    }

    public QueryResult executeQuery(String connectionName, String catalog, String sqlQuery, Object... params) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        QueryResult queryResult = jdbcTemplate.query(sqlQuery, new ResultSetExtractor<QueryResult>() {
            @Override
            public QueryResult extractData(ResultSet rs) throws SQLException, DataAccessException {
                ResultSetMetaData md = rs.getMetaData();
                List<Map<String, Object>> records = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> record = new HashMap<>();
                    for (int i = 0; i < md.getColumnCount(); i++) {
                        String fieldName = md.getColumnName(i + 1);
                        Object value = rs.getObject(i + 1);
                        if(!(value instanceof byte[])) { // Skip BLOBs
                        	record.put(fieldName, value);
                        }
                    }
                    records.add(record);
                }
                List<ColumnMetaData> metaData = new ArrayList<>();
                for (int i = 0; i < md.getColumnCount(); i++) {
                    metaData.add(ColumnMetaData.builder()
                            .name(md.getColumnName(i + 1))
                            .type(md.getColumnTypeName(i + 1))
                            .build());

                }
                QueryResult queryResult = QueryResult.builder().metaData(metaData).data(records).build();
                return queryResult;
            }
        }, params);
        return queryResult;
    }

    public List<ConnectionState> getConnections() {
        return configService.getConnectionFolders().stream().map(f -> {
            ConnectionState state = ConnectionState.builder()
                    .name(f.getName())
                    .connected(connections.containsKey(f.getName()))
                    .build();
            if(state.isConnected()) {
                DataSource ds = connections.get(state.getName());
                Connection connection = null;
                try {
                    connection = ds.getConnection();

                    DatabaseMetaData md = connection.getMetaData();
                    state.setDatabaseProductName(md.getDatabaseProductName());
                    state.setDatabaseProductVersion(md.getDatabaseProductVersion());
                    state.setDatabaseMajorVersion(md.getDatabaseMajorVersion());
                    state.setDatabaseMinorVersion(md.getDatabaseMinorVersion());
                } catch (SQLException e) {
                    log.error(String.format("Error getting driver name info for connection %s", state.getName()), e);
                } finally {
                    if(connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            log.error(String.format("Error closing connection %s", state.getName()), e);
                        }
                    }
                }

            }
            return state;
        }).collect(Collectors.toList());
    }

    public List<CatalogMetadata> getConnectionCatalogs(String connectionName) throws SQLException, IOException {
        HikariDataSource ds = connections.get(connectionName);
        ConnectionConfig configConnection = configService.getConnection(connectionName);
        if(ds == null) {
            ds = new HikariDataSource();
            ds.setDriverClassName(configConnection.getDriver());
            ds.setJdbcUrl(configConnection.getUrl());
            ds.setUsername(configConnection.getUser());
            ds.setPassword(configConnection.getPassword());
            connections.put(connectionName, ds);
        }
        JdbcTemplate template = new JdbcTemplate(ds);
        List<String> excludeCatalogs = ObjectUtils.firstNonNull(configConnection.getExcludeCatalogs(), Collections.emptyList());
        
        List<String> catalogNames = template.queryForList("SELECT datname as name FROM pg_database WHERE datistemplate = false", String.class);

        List<CatalogMetadata> catalogs = catalogNames.stream()
        		.filter(catalog -> !excludeCatalogs.contains(catalog))
        		.map(CatalogMetadata::new).collect(Collectors.toList());
        return catalogs;

        /*
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet catalogsRs = metadata.getCatalogs();
            List<CatalogMetadata> catalogs = new ArrayList<>();
            while (catalogsRs.next()) {
                catalogs.add(CatalogMetadata.builder().name(catalogsRs.getString("TABLE_CAT")).build());
            }
            return catalogs;
        }
        */
    }

    protected synchronized DataSource getDataSource(String connectionName, String catalog) throws IOException {
        String key = connectionName + "/" + catalog;
        HikariDataSource ds = connections.get(key);
        if(ds == null) {
            ConnectionConfig configConnection = configService.getConnection(connectionName);

            ConnectionUrl connectionUrl = parseConnectionString(configConnection.getUrl());
            connectionUrl.setCatalog(catalog);
            String jdbcUrl = connectionUrl.toUrl();

            ds = new HikariDataSource();
            ds.setDriverClassName(configConnection.getDriver());
            ds.setJdbcUrl(jdbcUrl);
            ds.setUsername(configConnection.getUser());
            ds.setPassword(configConnection.getPassword());
            connections.put(key, ds);
        }
        return ds;
    }

    public List<SchemaMetadata> getSchemas(String connectionName, String catalog) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet schemasRs = metadata.getSchemas();
            List<SchemaMetadata> schemas = new ArrayList<>();
            while(schemasRs.next()) {
                schemas.add(SchemaMetadata.builder()
                        .name(schemasRs.getString("TABLE_SCHEM"))
                        .build());
            }
            return schemas;
        }
    }

    public List<TableMetadata> getTables(String connectionName, String catalog, String schema) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet tablesRs = metadata.getTables(catalog, schema, null, null);
            List<TableMetadata> tables = new ArrayList<>();
            Map<String, TableMetadata> mapTables = new HashMap<>();
            while (tablesRs.next()) {
                String type = tablesRs.getString("TABLE_TYPE");
                String tableSchema = tablesRs.getString("TABLE_SCHEM");

                if ("TABLE".equals(type) && tableSchema.equals(tableSchema)) {
                    TableMetadata table = TableMetadata.builder()
                            .name(tablesRs.getString("TABLE_NAME"))
                            .schema(tablesRs.getString("TABLE_SCHEM"))
                            .type(type)
                            .build();
                    tables.add(table);
                    mapTables.put(table.getName(), table);
                }
            }

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
            return tables;
        }
    }

    public List<ColumnMetaData> getColumns(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
            List<ColumnMetaData> columns = new ArrayList<>();
            while (columnsRs.next()) {
                columns.add(ColumnMetaData.builder()
                        .name(columnsRs.getString("COLUMN_NAME"))
                        .type(columnsRs.getString("TYPE_NAME"))
                        .typeId(columnsRs.getInt("DATA_TYPE"))
                        .size(columnsRs.getInt("COLUMN_SIZE"))
                        .build());
            }
            ResultSet pkRs = metadata.getPrimaryKeys(catalog, schema, table);
            while(pkRs.next()) {
                String colName = pkRs.getString("COLUMN_NAME");
                columns.stream().filter(c ->c.getName().equals(colName)).findFirst().get().setPrimaryKey(true);
            }
            return columns;
        }
    }

    public List<ForeignKeyMetaData> getDetailForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        try (Connection connection = ds.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            ResultSet indexRs = metadata.getImportedKeys(catalog, schema, table);
            List<ForeignKeyMetaData> indexes = new ArrayList<>();
            while (indexRs.next()) {
                indexes.add(ForeignKeyMetaData.builder()
                        .name(indexRs.getString("FK_NAME"))
                        .masterTable(indexRs.getString("PKTABLE_NAME"))
                        .masterSchema(indexRs.getString("PKTABLE_SCHEM"))
                        .detailsTable(indexRs.getString("FKTABLE_NAME"))
                        .detailsSchema(indexRs.getString("FKTABLE_SCHEM"))
                        .pkFieldNameInMasterTable(indexRs.getString("PKCOLUMN_NAME"))
                        .fkFieldNameInDetailsTable(indexRs.getString("FKCOLUMN_NAME"))
                        .updateRule(indexRs.getString("UPDATE_RULE"))
                        .deleteRule(indexRs.getString("DELETE_RULE"))
                        .build());
            }
            return indexes;
        }
    }

    public List<ForeignKeyMetaData> getMasterForeignKeys(String connectionName, String catalog, String schema, String table) throws SQLException, IOException {
        List<ForeignKeyMetaData> indexes = getMasterForeignKeysForAllTables(connectionName, catalog, schema);
        return indexes.stream().filter(m -> table.equals(m.getMasterTable())).collect(Collectors.toList());
    }

    protected List<ForeignKeyMetaData> getMasterForeignKeysForAllTables(String connectionName, String catalog, String schema) throws SQLException, IOException {
        String key = String.join("#", connectionName, catalog, schema);

        List<ForeignKeyMetaData> indexes = null;
        synchronized (this.indexesCache) {
            indexes = this.indexesCache.get(key);
            if(indexes == null) {
                DataSource ds = getDataSource(connectionName, catalog);
                try (Connection connection = ds.getConnection()) {
                    DatabaseMetaData metadata = connection.getMetaData();
                    List<TableMetadata> tables = getTables(connectionName, catalog, schema);
                    indexes = new ArrayList<>();
                    for (TableMetadata tableMetadata : tables) {
                        ResultSet indexRs = metadata.getImportedKeys(catalog, schema, tableMetadata.getName());

                        while (indexRs.next()) {
                            indexes.add(ForeignKeyMetaData.builder()
                                    .name(indexRs.getString("FK_NAME"))
                                    .masterTable(indexRs.getString("PKTABLE_NAME"))
                                    .masterSchema(indexRs.getString("PKTABLE_SCHEM"))
                                    .detailsTable(indexRs.getString("FKTABLE_NAME"))
                                    .detailsSchema(indexRs.getString("FKTABLE_SCHEM"))
                                    .pkFieldNameInMasterTable(indexRs.getString("PKCOLUMN_NAME"))
                                    .fkFieldNameInDetailsTable(indexRs.getString("FKCOLUMN_NAME"))
                                    .updateRule(indexRs.getString("UPDATE_RULE"))
                                    .deleteRule(indexRs.getString("DELETE_RULE"))
                                    .build());
                        }
                    }
                }
                this.indexesCache.put(key, indexes);
            }
        }
        return indexes;

    }

    public QueryResult getTableData(String connectionName, String catalog, String schema, String table, String condition, String order, boolean asc) throws SQLException, IOException {
        return executeQuery(connectionName, catalog,"select * " +
                "from " + schema + "." + table +
                (condition != null && condition.trim().length() > 0 ? " where " + condition : "") +
                (order != null && order.trim().length() > 0 ? " order by " + order + (asc ? " asc" : " desc") : "") +
                " limit 100");
    }

    public Set<String> getMissed(List list1, List list2, Function<Object, String> getName) {
        Set<String> tableSet1 = (Set<String>) list1.stream().map(getName).collect(Collectors.toSet());
        Set<String> tableSet2 = (Set<String>) list2.stream().map(getName).collect(Collectors.toSet());
        Set<String> missed = new HashSet<>(tableSet1);
        missed.removeAll(tableSet2);
        return missed;
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
                    .build();
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse url '?'", url));
        }
    }

    public Optional<ConnectionCatalog> lookupAlias(String alias) throws IOException, SQLException {
        for(File connectionFodler : configService.getConnectionFolders()) {
            String connection = connectionFodler.getName();
            ConnectionConfig connectionConfig = configService.getConnection(connection);
            if(connectionConfig.getAliasQuery() != null) {
                List<CatalogMetadata> catalogs = getConnectionCatalogs(connection);
                // for(CatalogMetadata catalog : catalogs)
                Optional<ConnectionCatalog> con = catalogs.stream().parallel().filter(catalog ->
                {
                    try {
                        DataSource ds = getDataSource(connection, catalog.getName());
                        JdbcTemplate template = new JdbcTemplate(ds);
                        Boolean isAlias = template.queryForObject(connectionConfig.getAliasQuery(), Boolean.class, alias);
                        return isAlias;
                    } catch (Exception e) {
                        log.warn(String.format("Error executing lookup sql '%s'", connectionConfig.getAliasQuery()), e);
                    }
                    return false;
                }).findFirst().map(catalog -> ConnectionCatalog.builder()
                        .connection(connection)
                        .catalog(catalog.getName())
                        .build());
                if(con.isPresent()) {
                    return con;
                }
            }
        }
        return Optional.empty();
    }

    public Map<String, Integer> getMasterForeignKeyInfos(String connectionName, String catalog, String schema, String table, String id) throws IOException, SQLException {
        Map<String, Integer> infos = new HashMap();
        List<ForeignKeyMetaData> keys = getMasterForeignKeys(connectionName, catalog, schema, table);
        List<ColumnMetaData> cols = getColumns(connectionName, catalog, schema, table);
        List<ColumnMetaData> pkCols = cols.stream().filter(ColumnMetaData::isPrimaryKey).collect(Collectors.toList());
        if(pkCols.size() > 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) with more than 1 PK field", table));
        }
        if(pkCols.size() < 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) without PK field", table));
        }
        ColumnMetaData pkCol = pkCols.get(0);

        Object oId = id;
        if(pkCol.getTypeId() == Types.BIGINT
                || pkCol.getTypeId() == Types.INTEGER
                || pkCol.getTypeId() == Types.NUMERIC) {
            oId = Integer.parseInt(String.valueOf(id));
        }
        DataSource ds = getDataSource(connectionName, catalog);
        JdbcTemplate template = new JdbcTemplate(ds);
        for(ForeignKeyMetaData key : keys) {
            String sql = "select count(*) from \"" + key.getDetailsTable() + "\" where \"" + key.getFkFieldNameInDetailsTable() + "\" = ?";

            Integer count = template.queryForObject(sql, Integer.class, oId);
            infos.put(key.getName(), count);
        }

        return infos;
    }
    
    public QueryInfo getQueryInfo(String connectionName, String catalog, String query) throws SQLException, IOException, StandardException {
    	QueryInfo info = QueryInfo.builder().query(query).build();
    	SQLParser parser = new SQLParser();
    	StatementNode sn = parser.parseStatement(query);
		List<String> tables = new ArrayList<>();
    	if(sn instanceof CursorNode) {
    		CursorNode cn = (CursorNode) sn;
    		ResultSetNode rsn = cn.getResultSetNode();
    		if(rsn instanceof SelectNode) {
    			SelectNode selectNode = (SelectNode) rsn;
    			FromList fromList = selectNode.getFromList();
    			for(int i = 0; i < fromList.size(); i++) {
    				FromTable fromTable = fromList.get(i);
    				TableName tableName = fromTable.getTableName();
    				tables.add(tableName.getTableName());
    			}
    		}
    	}
    	info.setTableNames(tables);

    	List<ForeignKeyMetaData> allMasterKeys = new ArrayList<>();
    	List<ForeignKeyMetaData> allDetailKeys = new ArrayList<>();
    	
    	for(String table : tables) {
    		allMasterKeys.addAll(getMasterForeignKeys(connectionName, catalog, null, table));
    		allDetailKeys.addAll(getDetailForeignKeys(connectionName, catalog, null, table));
    	}
		info.setMasterForeignKeys(allMasterKeys);
		info.setDetailForeignKeys(allDetailKeys);
    	
		return info ;
    }

	public byte[] getBinaryData(String connectionName, String catalog, String schema, String table, String column,
			String id) throws SQLException, IOException {
        List<ColumnMetaData> cols = getColumns(connectionName, catalog, schema, table);
        List<ColumnMetaData> pkCols = cols.stream().filter(ColumnMetaData::isPrimaryKey).collect(Collectors.toList());
        if(pkCols.size() > 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) with more than 1 PK field", table));
        }
        if(pkCols.size() < 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) without PK field", table));
        }
        ColumnMetaData pkCol = pkCols.get(0);

        Object oId = id;
        if(pkCol.getTypeId() == Types.BIGINT
                || pkCol.getTypeId() == Types.INTEGER
                || pkCol.getTypeId() == Types.NUMERIC) {
            oId = Integer.parseInt(String.valueOf(id));
        }
        String sql = "select " + column + " from " + schema + "." + table + " where " + pkCol.getName() + "=?";
        return getBinaryData(connectionName, catalog, sql, oId);
	}

	public byte[] getBinaryData(String connectionName, String catalog, String sql, Object...params) throws SQLException, IOException {
        DataSource ds = getDataSource(connectionName, catalog);
        JdbcTemplate template = new JdbcTemplate(ds);
        ResultSetExtractor<byte[]> rse = new ResultSetExtractor<byte[]>() {

			@Override
			public byte[] extractData(ResultSet rs) throws SQLException, DataAccessException {
				rs.next();
				return rs.getBytes(1);
			}
		};
		byte[] data = template.query(sql, rse, params);
		return data;
		
	}
			
}
