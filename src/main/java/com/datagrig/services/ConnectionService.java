package com.datagrig.services;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.cayenne.exp.parser.ExpressionParserConstants;
import org.apache.cayenne.exp.parser.ExpressionParserTokenManager;
import org.apache.cayenne.exp.parser.JavaCharStream;
import org.apache.cayenne.exp.parser.Token;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.CursorNode;
import com.akiban.sql.parser.FromList;
import com.akiban.sql.parser.FromTable;
import com.akiban.sql.parser.ResultSetNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SelectNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.TableName;
import com.datagrig.AppConfig;
import com.datagrig.ConnectionConfig;
import com.datagrig.pojo.CatalogMetadata;
import com.datagrig.pojo.ColumnMetaData;
import com.datagrig.pojo.ConnectionCatalog;
import com.datagrig.pojo.ConnectionState;
import com.datagrig.pojo.ForeignKeyMetaData;
import com.datagrig.pojo.QueryInfo;
import com.datagrig.pojo.QueryResult;
import com.datagrig.pojo.SequenceMetaData;
import com.datagrig.pojo.TableMetadata;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConnectionService {

	private static final Set<String> NULLABLE_VALUES = Collections.<String>unmodifiableSet(new HashSet<String>(Arrays.asList(new String[]{"YES", "yes", "+", "1"})));

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private ConfigService configService;
    
    @Autowired
    private DataSourceService dataSourceService;
    
    @Autowired
    private MetadataService metadataService;

    private Map<String, HikariDataSource> connections = new HashMap<>();

    /*
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
    */

    public QueryResult executeQuery(String connectionName, String catalog, int limit, int page, String sqlQuery, Object... params) throws SQLException, IOException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
        QueryResult queryResult = jdbcTemplate.query(sqlQuery + (limit > 0 ? " limit " + limit + " offset " + limit * page : ""), new ResultSetExtractor<QueryResult>() {
            @Override
            public QueryResult extractData(ResultSet rs) throws SQLException, DataAccessException {
                ResultSetMetaData md = rs.getMetaData();
                List<Map<String, Object>> records = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> record = new HashMap<>();
                    for (int i = md.getColumnCount(); i > 0; i--) {
                        String fieldName = md.getColumnName(i);
                        Object value = rs.getObject(i);
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
        if(limit > 0) {
        	int count = jdbcTemplate.queryForObject("select count(*) from (" + sqlQuery + ") as t", Integer.class, params);
        	int mod = count % limit;
        	int lastPage = (count - mod) / limit + (mod > 0 ? 1 : 0);
        	queryResult.setTotalCount(count);
        	queryResult.setPage(page);
        	queryResult.setLastPage(lastPage);
        	queryResult.setLimit(limit);
        }
        return queryResult;
    }

    public List<ConnectionState> getConnections(boolean brief) {
        List<File> conFiles = configService.getConnectionFolders();
        return conFiles.stream().map(f -> {
            ConnectionState state = ConnectionState.builder()
                    .name(f.getName())
                    .connected(connections.containsKey(f.getName()))
                    .build();
            if(!brief && state.isConnected()) {
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

    public QueryResult getTableData(String connectionName, String catalog, String schema, String table, String id) throws SQLException, IOException, StandardException {
    	List<ColumnMetaData> columns = metadataService.getColumns(connectionName, catalog, schema, table);
    	ColumnMetaData pkCol = columns.stream().filter(ColumnMetaData::isPrimaryKey).findFirst().orElseThrow(() -> new IllegalStateException(String.format("Can not find pk in table %s", table)));
    	String query = generateTableSelectQuery(connectionName, catalog, schema, table, "", null, true);
    	query = query + " WHERE " + String.format("\"%s\" = ?", pkCol.getName());
    	Object oId = id;
        if(pkCol.getTypeId() == Types.BIGINT
                || pkCol.getTypeId() == Types.INTEGER
                || pkCol.getTypeId() == Types.NUMERIC) {
            oId = Integer.parseInt(String.valueOf(id));
        }
        return executeQuery(connectionName, catalog, 0, 0, query, oId);
    }
    
    public QueryResult getTableData(String connectionName, String catalog, String schema, String table, String condition, int limit, int page, String order, boolean asc) throws SQLException, IOException, StandardException {
    	String query = generateTableSelectQuery(connectionName, catalog, schema, table, condition, order, asc);
        return executeQuery(connectionName, catalog, limit, page, query);
    }
    
    private String generateTableSelectQuery(String connectionName, String catalog, String schema, String table, String condition, String order, boolean asc) throws StandardException, SQLException, IOException {
    	String fromClause = "\"" + schema + "\".\"" + table + "\" as " + table + "0";
    	
    	if(condition != null && condition.length() > 0) {
	    	List<Token> condTokens = tokenizeSql(condition);
	    	
	    	for(Token condToken : condTokens) {
	    		if(condToken.kind == ExpressionParserConstants.PROPERTY_PATH) {
	    			String[] pathElements = condToken.image.split("\\.");
	    			if(pathElements.length >= 2) {
	    				ForeignKeyMetaData prevFk = null;
	    				String prevRefToTable = null;
		    			for(int i = 0; i < pathElements.length - 1; i++) {
		    				String propName = pathElements[i];
		    				String refToTable = i == 0 ? table : prevRefToTable; // prevFk.getMasterTable();
		    		    	List<ForeignKeyMetaData> fks = metadataService.getDetailForeignKeys(connectionName, catalog, schema, refToTable);
		    				List<ForeignKeyMetaData> fksToTable = fks.stream().filter(
		    						// fk -> fk.getMasterTable().equalsIgnoreCase(tableName) || fk.getFkFieldNameInDetailsTable().startsWith(tableName)
		    						fk -> fk.getFkFieldNameInDetailsTable().equalsIgnoreCase(propName + "_id") || propName.equalsIgnoreCase(fk.getAliasInDetailsTable())
		    						).collect(Collectors.toList());
		    				if(fksToTable.size() == 1) {
		    					ForeignKeyMetaData fkToTable = fksToTable.get(0);
		    					fromClause = fromClause + "\n  JOIN \"" + fkToTable.getMasterTable() + "\""  + " as " + (fkToTable.getMasterTable() + (i + 1)) + 
		    							" ON " + (refToTable + i) + "." + fkToTable.getFkFieldNameInDetailsTable() + " = " + (fkToTable.getMasterTable() + (i + 1)) + "." + fkToTable.getPkFieldNameInMasterTable(); 
		    					prevFk = fkToTable;
		    					prevRefToTable = fkToTable.getMasterTable();
		    					pathElements[i] = fkToTable.getMasterTable() + (i + 1);
		    				} else if(fksToTable.size() > 1){
		    					throw new IllegalArgumentException(String.format("Cannot resolve property name %s", propName));
		    				} else {
			    				refToTable = i == 0 ? table : prevRefToTable; //prevFk.getDetailsTable();
		    					// Trying to resolve through master fks
		    					List<ForeignKeyMetaData> masterFks = metadataService.getMasterForeignKeys(connectionName, catalog, schema, refToTable);
			    				List<ForeignKeyMetaData> masterFksToTable = masterFks.stream().filter(
			    						// fk -> fk.getMasterTable().equalsIgnoreCase(tableName) || fk.getFkFieldNameInDetailsTable().startsWith(tableName)
			    						fk -> (fk.getDetailsTable() + "s").equalsIgnoreCase(propName) || propName.equalsIgnoreCase(fk.getAliasInMasterTable())
			    						).collect(Collectors.toList());
			    				
			    				String finalRefToTable = refToTable;
			    				if(masterFksToTable.size() > 1) {
			    					masterFksToTable = masterFksToTable.stream().filter(fk -> fk.getFkFieldNameInDetailsTable().equalsIgnoreCase(finalRefToTable + "_id")).collect(Collectors.toList());
			    				}
			    				if(masterFksToTable.size() == 1) {
			    					ForeignKeyMetaData fkToTable = masterFksToTable.get(0);
			    					fromClause = fromClause + "\n  JOIN \"" + fkToTable.getDetailsTable() + "\""  + " as " + (fkToTable.getDetailsTable() + (i + 1)) + 
			    							" ON " + (refToTable + i) + "." + fkToTable.getPkFieldNameInMasterTable() + " = " + (fkToTable.getDetailsTable() + (i + 1)) + "." + fkToTable.getFkFieldNameInDetailsTable(); 
			    					prevFk = fkToTable;
			    					prevRefToTable = fkToTable.getDetailsTable();
			    					pathElements[i] = fkToTable.getDetailsTable() + (i + 1);
			    					
			    				} else {
			    					throw new IllegalArgumentException(String.format("Cannot resolve property name %s", propName));
			    				}
		    					
		    				}
		    			}
		    			condToken.image = pathElements[pathElements.length - 2] + "." + pathElements[pathElements.length - 1];
	    			} else {
	    				condToken.image = table + "0." + pathElements[pathElements.length - 1];
	    			}
	    		}
	    	}
    		condition = condTokens.stream().map(t -> t.image).collect(Collectors.joining(" ")); 
	    	
    	}

		String simpleSql = "select distinct " + table + "0.* from " + fromClause +
                (condition != null && condition.trim().length() > 0 ? " where " + condition : "") +
                (order != null && order.trim().length() > 0 ? " order by " + table + "0." + order + (asc ? " asc" : " desc") : "");
    	log.info("SQL = " + simpleSql);
    	return simpleSql;
    	
    }
    
    protected List<Token> tokenizeSql(String sql) {
    	Reader reader = new StringReader(sql);
    	JavaCharStream stream = new JavaCharStream(reader);
		ExpressionParserTokenManager tokenManager = new ExpressionParserTokenManager(stream);
		List<Token> tokens = new ArrayList<>();
		Token t;
		do {
			t = tokenManager.getNextToken();
			if(t.kind != ExpressionParserConstants.EOF) {
				tokens.add(t);
			}
		} while(t.kind != ExpressionParserConstants.EOF);
		return tokens;
    }

    public Set<String> getMissed(List list1, List list2, Function<Object, String> getName) {
        Set<String> tableSet1 = (Set<String>) list1.stream().map(getName).collect(Collectors.toSet());
        Set<String> tableSet2 = (Set<String>) list2.stream().map(getName).collect(Collectors.toSet());
        Set<String> missed = new HashSet<>(tableSet1);
        missed.removeAll(tableSet2);
        return missed;
    }

    public Optional<ConnectionCatalog> lookupAlias(String alias) throws IOException, SQLException {
        for(File connectionFodler : configService.getConnectionFolders()) {
            String connection = connectionFodler.getName();
            ConnectionConfig connectionConfig = configService.getConnection(connection);
            if(connectionConfig.getAliasQuery() != null) {
                List<CatalogMetadata> catalogs = metadataService.getConnectionCatalogs(connection);
                // for(CatalogMetadata catalog : catalogs)
                Optional<ConnectionCatalog> con = catalogs.stream().parallel().filter(catalog ->
                {
                    try {
                        DataSource ds = dataSourceService.getDataSource(connection, catalog.getName());
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

    public Map<String, Integer> getMasterForeignKeyInfos(String connectionName, String catalog, String schema, String table, String id) throws IOException, SQLException, InterruptedException, ExecutionException {
        Map<String, Integer> infos = new HashMap();
        List<ForeignKeyMetaData> keys = metadataService.getMasterForeignKeys(connectionName, catalog, schema, table);
        List<ColumnMetaData> cols = metadataService.getColumns(connectionName, catalog, schema, table);
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
        Object finalOid = oId;
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        JdbcTemplate template = new JdbcTemplate(ds);
        //for(ForeignKeyMetaData key : keys) {
        ForkJoinPool customThreadPool = new ForkJoinPool(4);
        Object object = customThreadPool.submit(
	          () -> 
	        
	          keys.parallelStream().forEach(key -> {	
	            String sql = "select count(*) from \"" + key.getDetailsTable() + "\" where \"" + key.getFkFieldNameInDetailsTable() + "\" = ?";
	
	            Integer count = template.queryForObject(sql, Integer.class, finalOid);
	            infos.put(key.getName(), count);
        })).get();

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
    		allMasterKeys.addAll(metadataService.getMasterForeignKeys(connectionName, catalog, null, table));
    		allDetailKeys.addAll(metadataService.getDetailForeignKeys(connectionName, catalog, null, table));
    	}
		info.setMasterForeignKeys(allMasterKeys);
		info.setDetailForeignKeys(allDetailKeys);
    	
		return info ;
    }

	public byte[] getBinaryData(String connectionName, String catalog, String schema, String table, String column,
			String id) throws SQLException, IOException {
        List<ColumnMetaData> cols = metadataService.getColumns(connectionName, catalog, schema, table);
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
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
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

	public List<SequenceMetaData> getSequences(String connectionName, String catalog, String schema) throws IOException {
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        JdbcTemplate template = new JdbcTemplate(ds);
        List<String> seqNames = template.queryForList("SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'", String.class);
        String select = seqNames.stream().map(sn -> sn + ".last_value as " + sn + "_lv").collect(Collectors.joining(", "));
        String from = seqNames.stream().collect(Collectors.joining(", "));

        Map<String, Object> lvs = template.queryForMap("select " + select + " from " + from);
		List<SequenceMetaData> sequences = seqNames.stream().map(n -> {
			Long v = (Long) lvs.get(n + "_lv");
			return SequenceMetaData.builder()
					.name(n)
					.value(v)
					.build();
		}).collect(Collectors.toList());
		return sequences;
	}

}
