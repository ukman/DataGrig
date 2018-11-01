package com.datagrig.services;

import java.io.*;
import java.sql.*;
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

import com.datagrig.pojo.*;
import com.datagrig.sql.SQLTokenizer;
import org.apache.cayenne.exp.parser.ExpressionParserConstants;
import org.apache.cayenne.exp.parser.ExpressionParserTokenManager;
import org.apache.cayenne.exp.parser.JavaCharStream;
import org.apache.cayenne.exp.parser.Token;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.RequestParam;

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
import com.jcraft.jsch.JSchException;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ConnectionService {

	private static final Set<String> NULLABLE_VALUES = Collections.<String>unmodifiableSet(new HashSet<String>(Arrays.asList(new String[]{"YES", "yes", "+", "1"})));
	private static final Set<String> NOT_PATH_TOKENS = Collections.<String>unmodifiableSet(new HashSet<String>(Arrays.asList(new String[]{"is"})));

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

    public PagingInfo getTablePagingInfo(String connectionName, String catalog, String schema, String table, int limit, int page, String condition) throws SQLException, IOException, StandardException, JSchException {
    	String query = generateTableSelectQuery(connectionName, catalog, schema, table, condition, null, true);
    	return getPagingInfo(connectionName, catalog, limit, page, query);
    }
    
    public PagingInfo getPagingInfo(String connectionName, String catalog, int limit, int page, String sqlQuery, Object... params) throws SQLException, IOException, JSchException {
        if(limit > 0) {
        	DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);

            int count = jdbcTemplate.queryForObject("select count(*) from (" + sqlQuery + ") as t", Integer.class, params);
        	int mod = count % limit;
        	int lastPage = (count - mod) / limit + (mod > 0 ? 1 : 0);
        	return PagingInfo.builder().totalCount(count)
        		.page(page)
        		.lastPage(lastPage)
        		.limit(limit)
        		.build();
        	
        }
    	throw new IllegalArgumentException("Limit param cannot be 0");
    }

    public QueryResult executeQuery(String connectionName, String catalog, int limit, int page, String sqlQuery, Object... params) throws SQLException, IOException, JSchException {
    	StopWatch stopWatch = new StopWatch("executeQuery");

        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
    	stopWatch.start("Calculating count");
    	int count = 0;
        stopWatch.stop();

    	stopWatch.start("Getting Data");
    	log.info("execute Query started");
		QueryResult queryResult = jdbcTemplate.query(sqlQuery + (limit > 0 ? " limit " + limit + " offset " + limit * page : ""), new ResultSetExtractor<QueryResult>() {
			@Override
			public QueryResult extractData(ResultSet rs) throws SQLException, DataAccessException {
				stopWatch.stop();
				stopWatch.start("Extracting Data");
				ResultSetMetaData md = rs.getMetaData();
				List<Map<String, Object>> records = new ArrayList<>();
				while (rs.next()) {
					Map<String, Object> record = new HashMap<>();
					for (int i = md.getColumnCount(); i > 0; i--) {
						String fieldName = md.getColumnName(i);
						int typeId = md.getColumnType(i);
						if (!isBinary(typeId)) { // Skip BLOBs
							Object value = rs.getObject(i);
							if (value instanceof Array) {
								Array array = (Array) value;
								Object arr = array.getArray();
								log.info("Arrya = " + arr);
								value = arr;
							}
							if (value != null && !(value instanceof Serializable)) {
								log.error("value is not serialilzable " + value + " " + value.getClass());
							}
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
        stopWatch.stop();
        log.info(String.format("executeQuery \n%s", stopWatch.prettyPrint()));
        
        return queryResult;
    }

    public boolean isBinary(int typeId) {
    	return typeId == Types.BINARY 
    			|| typeId == Types.VARBINARY
    			|| typeId == Types.LONGVARBINARY;
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

    public QueryResult getTableData(String connectionName, String catalog, String schema, String table, String id) throws SQLException, IOException, StandardException, JSchException {
    	StopWatch stopWatch = new StopWatch();
    	stopWatch.start();
    	List<ColumnMetaData> columns = metadataService.getColumns(connectionName, catalog, schema, table);
    	log.info(String.format("getTableData time 1 : %f sec", stopWatch.getTotalTimeSeconds()));
    	ColumnMetaData pkCol = columns.stream().filter(ColumnMetaData::isPrimaryKey).findFirst().orElseThrow(() -> new IllegalStateException(String.format("Can not find pk in table %s", table)));
    	String query = generateTableSelectQuery(connectionName, catalog, schema, table, "", null, true);
    	log.info(String.format("getTableData time 2 : %f sec", stopWatch.getTotalTimeSeconds()));
    	query = query + " WHERE " + String.format("\"%s\" = ?", pkCol.getName());
    	Object oId = id;
        if(pkCol.getTypeId() == Types.BIGINT
                || pkCol.getTypeId() == Types.INTEGER
                || pkCol.getTypeId() == Types.NUMERIC) {
            oId = Integer.parseInt(String.valueOf(id));
        }
        QueryResult data = executeQuery(connectionName, catalog, 0, 0, query, oId);
    	log.info(String.format("getTableData time 3 : %f sec", stopWatch.getTotalTimeSeconds()));
    	return data;
    }
    
    public QueryResult getTableData(String connectionName, String catalog, String schema, String table, String condition, int limit, int page, String order, boolean asc) throws SQLException, IOException, StandardException, JSchException {
    	String query = generateTableSelectQuery(connectionName, catalog, schema, table, condition, order, asc);
        return executeQuery(connectionName, catalog, limit, page, query);
    }
    
    private String generateTableSelectQuery(String connectionName, String catalog, String schema, String table, String condition, String order, boolean asc) throws StandardException, SQLException, IOException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
		String q = metadataService.getIdentifierQuoteString(connectionName, catalog);

		String schemaWithDotAndQuotes = schema == null || isMysql ? "" : quote(q, schema) + ".";
    	String fromClause = "" + schemaWithDotAndQuotes + quote(q, table) + " as " + quote(q,table + "0");

    	List<ColumnMetaData> columns = metadataService.getColumns(connectionName, catalog, schema, table);
    	
    	List<ColumnMetaData> pkColumns = columns.stream().filter(ColumnMetaData::isPrimaryKey).collect(Collectors.toList());

		if(condition != null && condition.length() > 0 && !condition.startsWith(" ") && pkColumns.size() >= 1) {
			List<SQLTokenizer.Token> condTokens = tokenizeSql(condition, connectionName, catalog);
	    	
	    	
	    	for(SQLTokenizer.Token condToken : condTokens) {
	    		if(condToken.getType() == SQLTokenizer.SQLTokenType.ID) { // ExpressionParserConstants.PROPERTY_PATH && !NOT_PATH_TOKENS.contains(condToken.image.toLowerCase())) {
	    			String[] pathElements = condToken.getToken().split("\\.");
	    			if(pathElements.length >= 2) {
	    				ForeignKeyMetaData prevFk = null;
	    				String prevRefToTable = null;
		    			for(int i = 0; i < pathElements.length - 1; i++) {
		    				String pathElement = pathElements[i];
		    				String propName = unquote(q, pathElement);
		    				String refToTable = i == 0 ? table : prevRefToTable; // prevFk.getMasterTable();
		    		    	List<ForeignKeyMetaData> fks = metadataService.getDetailForeignKeys(connectionName, catalog, schema, refToTable);
		    				List<ForeignKeyMetaData> fksToTable = fks.stream().filter(
		    						// fk -> fk.getMasterTable().equalsIgnoreCase(tableName) || fk.getFkFieldNameInDetailsTable().startsWith(tableName)
		    						fk -> fk.getFkFieldNameInDetailsTable().equalsIgnoreCase(propName + "_id") || propName.equalsIgnoreCase(fk.getAliasInDetailsTable())
		    						).collect(Collectors.toList());
		    				if(fksToTable.size() == 1) {
		    					ForeignKeyMetaData fkToTable = fksToTable.get(0);
		    					fromClause = fromClause + "\n  JOIN " + quote(q, fkToTable.getMasterTable()) + ""  + " as " + quote(q,(fkToTable.getMasterTable() + (i + 1))) +
		    							" ON " + quote(q, refToTable + i) + "." + quote(q, fkToTable.getFkFieldNameInDetailsTable()) + " = " +
										quote(q, fkToTable.getMasterTable() + (i + 1)) + "." + quote(q, fkToTable.getPkFieldNameInMasterTable());
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
			    						fk -> {
			    							String s = fk.getDetailsTable() + "s";
			    							String qs = quote(q, s);
			    							String s2 = fk.getAliasInMasterTable();
											String qs2 = quote(q, s2);

			    							return s.equalsIgnoreCase(propName) || qs.equalsIgnoreCase(propName)
													|| propName.equalsIgnoreCase(s2)
													|| propName.equalsIgnoreCase(qs2);
										}
			    						).collect(Collectors.toList());
			    				
			    				String finalRefToTable = refToTable;
			    				if(masterFksToTable.size() > 1) {
			    					masterFksToTable = masterFksToTable.stream().filter(fk -> fk.getFkFieldNameInDetailsTable().equalsIgnoreCase(finalRefToTable + "_id")).collect(Collectors.toList());
			    				}
			    				if(masterFksToTable.size() == 1) {
			    					ForeignKeyMetaData fkToTable = masterFksToTable.get(0);
			    					fromClause = fromClause + "\n  JOIN " + quote(q, fkToTable.getDetailsTable()) + " as " + quote(q, fkToTable.getDetailsTable() + (i + 1)) +
			    							" ON " + quote(q, refToTable + i) + "." + quote(q, fkToTable.getPkFieldNameInMasterTable()) + " = " + quote(q, fkToTable.getDetailsTable() + (i + 1)) + "." + quote(q, fkToTable.getFkFieldNameInDetailsTable());
			    					prevFk = fkToTable;
			    					prevRefToTable = fkToTable.getDetailsTable();
			    					pathElements[i] = fkToTable.getDetailsTable() + (i + 1);
			    					
			    				} else {
			    					throw new IllegalArgumentException(String.format("Cannot resolve property name '%s'", propName));
			    				}
		    					
		    				}
		    			}
		    			condToken.setToken(pathElements[pathElements.length - 2] + "." + pathElements[pathElements.length - 1]);
	    			} else {
	    				String pe = pathElements[pathElements.length - 1];
	    				log.info(String.format("Check if token %s should be modified.", condToken.getToken()));
	    				columns.stream().filter(c -> c.getName().equalsIgnoreCase(pe)).findFirst().ifPresent(c -> {
		    				condToken.setToken(table + "0." + pe);
	    				});
	    			}
	    		}
	    	}
    		condition = condTokens.stream().map(t -> t.getToken()).collect(Collectors.joining(" "));
	    	
    	}

//		String simpleSql = "select distinct " + table + "0.* from " + fromClause +
//                (condition != null && condition.trim().length() > 0 ? " where " + condition : "") +
//                (order != null && order.trim().length() > 0 ? " order by " + table + "0." + order + (asc ? " asc" : " desc") : "");
    	String fullCondition = (condition != null && condition.trim().length() > 0 ?
    				(condition.startsWith(" ") /* || pkColumns.size() != 1 */ ?
    						" where " + condition :
    						" where (" + pkColumns.stream().map(ColumnMetaData::getName).collect(Collectors.joining(", ")) + ") in (select " +
									pkColumns.stream().map(c -> table + "0." + c.getName()).collect(Collectors.joining(", ")) +
									" from " + fromClause + " where " + condition + ") "
					)
				: "");

    	String tableFields;
    	boolean hasBinary = columns.stream().anyMatch(ColumnMetaData::isBinary);
    	if(hasBinary) {
    		// Skip binary fields
    		tableFields = columns.stream().filter(c -> !c.isBinary()).map(ColumnMetaData::getName).collect(Collectors.joining(", "));
    	} else {
    		tableFields = "*";
    	}
    	
		String simpleSql = "select " + tableFields + " from " + table +
				fullCondition +
				(order != null && order.trim().length() > 0 ? " order by " + order + (asc ? " asc" : " desc") : "");
    	log.info("SQL = " + simpleSql);
    	return simpleSql;
    	
    }

    protected String quote(String quote, String name) {
    	return quote + name + quote;
	}

    protected String unquote(String quote, String name) {
    	if(name.startsWith(quote) && name.endsWith(quote)) {
    		return name.substring(quote.length(), name.length() - quote.length());
		}
    	return name;
	}

    protected List<SQLTokenizer.Token> tokenizeSql(String sql, String connectionName, String catalog) throws IOException, JSchException, SQLException {
    	/*
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
		//*/

		DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
		try (Connection con = ds.getConnection()) {
			DatabaseMetaData metadata = con.getMetaData();
			SQLTokenizer tokenizer = new SQLTokenizer(metadata);
			return tokenizer.getTokens(sql);
		}
    }

    public Set<String> getMissed(List list1, List list2, Function<Object, String> getName) {
        Set<String> tableSet1 = (Set<String>) list1.stream().map(getName).collect(Collectors.toSet());
        Set<String> tableSet2 = (Set<String>) list2.stream().map(getName).collect(Collectors.toSet());
        Set<String> missed = new HashSet<>(tableSet1);
        missed.removeAll(tableSet2);
        return missed;
    }

    public Optional<ConnectionCatalog> lookupAlias(String alias) throws IOException, SQLException, JSchException {
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

    public Map<String, Integer> getMasterForeignKeyInfos(String connectionName, String catalog, String schema, String table, String id, String path) throws IOException, SQLException, InterruptedException, ExecutionException, JSchException {
		boolean isMysql = dataSourceService.isMySQLDB(connectionName);
		schema = isMysql ? null : schema;

		Map<String, Integer> infos = new HashMap();
		List<SchemaTable> pathItems = resolvePath(connectionName, catalog, schema, table, path);
		String q = metadataService.getIdentifierQuoteString(connectionName, catalog);
		List<String> inversePath = inversePath(connectionName, catalog, schema, table, path).stream().map(s -> quote(q, s)).collect(Collectors.toList());
		SchemaTable schemaTable = pathItems.get(pathItems.size() - 1);
        List<ForeignKeyMetaData> keys = metadataService.getMasterForeignKeys(connectionName, catalog, schemaTable.getSchema(), schemaTable.getTable());
        List<ColumnMetaData> cols = metadataService.getColumns(connectionName, catalog, schemaTable.getSchema(), schemaTable.getTable());
        List<ColumnMetaData> pkCols = cols.stream().filter(ColumnMetaData::isPrimaryKey).collect(Collectors.toList());
        if(pkCols.size() > 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) with more than 1 PK field", schemaTable.getTable()));
        }
        if(pkCols.size() < 1) {
            throw new IllegalArgumentException(String.format("Can not process table (%s) without PK field", schemaTable.getTable()));
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
				try {

					// String sql = "select count(*) from \"" + key.getDetailsTable() + "\" where \"" + key.getFkFieldNameInDetailsTable() + "\" = ?";
					String inversePathS = String.join(".", inversePath);
					inversePathS = inversePathS.length() > 0 ? inversePathS + "." : inversePathS;
					String condition = quote(q,key.getAliasInDetailsTable()) + "." + inversePathS + "id = ?";
					String tableSql = generateTableSelectQuery(connectionName, catalog, key.getDetailsSchema(), key.getDetailsTable(), condition, null, true);
					// String sql = "select count(*) from " + key.getDetailsTable() + " where " + key.getFkFieldNameInDetailsTable() + " = ?";
					String sql = "select count(t.id) from (" + tableSql + ") as t";

					Integer count = template.queryForObject(sql, Integer.class, finalOid);
					infos.put(key.getName(), count);
				}catch (Exception e) {
					log.error("Error", e);
				}
        })).get();

        return infos;
    }

	/**
	 * Resolves path for a table. E.g. table 'person' in schema 'public' has foreign key field 'company_id' to table 'company', it means path 'company' should return array
	 * [{schema:'public', table:'person'}, {schema:'public', table:'company'}]
	 * @param connectionName
	 * @param catalog
	 * @param schema
	 * @param table
	 * @param path
	 * @return
	 * @throws JSchException
	 * @throws SQLException
	 * @throws IOException
	 */
	public List<SchemaTable> resolvePath(String connectionName, String catalog, String schema, String table, String path) throws JSchException, SQLException, IOException {
		String[] pathItems = path == null ? new String[0] : path.split("\\.");
		List<SchemaTable> res = new ArrayList<>();
		SchemaTable curSchemaTable = SchemaTable.builder().schema(schema).table(table).build();
		res.add(curSchemaTable);

		for(String pathItem : pathItems) {
			List<ForeignKeyMetaData> dfks = metadataService.getDetailForeignKeys(connectionName, catalog, curSchemaTable.getSchema(), curSchemaTable.getTable());
			Optional<ForeignKeyMetaData> dfkOpt = dfks.stream().filter(fk -> fk.getAliasInDetailsTable().equals(pathItem)).findFirst();
			if(dfkOpt.isPresent()) {
				ForeignKeyMetaData dfk = dfkOpt.get();
				curSchemaTable = SchemaTable.builder().schema(dfk.getMasterSchema()).table(dfk.getMasterTable()).build();
				res.add(curSchemaTable);
			} else {
				List<ForeignKeyMetaData> mfks = metadataService.getMasterForeignKeys(connectionName, catalog, curSchemaTable.getSchema(), curSchemaTable.getTable());
				ForeignKeyMetaData mfk = mfks.stream().filter(fk -> fk.getAliasInMasterTable().equals(pathItem)).findFirst().orElseThrow(() -> {
					return new IllegalArgumentException(String.format("Cannot resolve '%s'", pathItem));
				});
				curSchemaTable = SchemaTable.builder().schema(mfk.getDetailsSchema()).table(mfk.getDetailsTable()).build();
				res.add(curSchemaTable);
			}
		}
		return res;
	}

	public List<String> inversePath(String connectionName, String catalog, String schema, String table, String path) throws JSchException, SQLException, IOException {
		String[] pathItems = path == null ? new String[0] : path.split("\\.");
		List<String> res = new ArrayList<>();
		SchemaTable curSchemaTable = SchemaTable.builder().schema(schema).table(table).build();

		for(String pathItem : pathItems) {
			List<ForeignKeyMetaData> dfks = metadataService.getDetailForeignKeys(connectionName, catalog, curSchemaTable.getSchema(), curSchemaTable.getTable());
			Optional<ForeignKeyMetaData> dfkOpt = dfks.stream().filter(fk -> fk.getAliasInDetailsTable().equals(pathItem)).findFirst();
			if(dfkOpt.isPresent()) {
				ForeignKeyMetaData dfk = dfkOpt.get();
				curSchemaTable = SchemaTable.builder().schema(dfk.getMasterSchema()).table(dfk.getMasterTable()).build();
				res.add(0, dfk.getAliasInMasterTable());
			} else {
				List<ForeignKeyMetaData> mfks = metadataService.getMasterForeignKeys(connectionName, catalog, curSchemaTable.getSchema(), curSchemaTable.getTable());
				ForeignKeyMetaData mfk = mfks.stream().filter(fk -> fk.getAliasInMasterTable().equals(pathItem)).findFirst().orElseThrow(() -> {
					return new IllegalArgumentException(String.format("Cannot resolve '%s'", pathItem));
				});
				curSchemaTable = SchemaTable.builder().schema(mfk.getDetailsSchema()).table(mfk.getDetailsTable()).build();
				res.add(0, mfk.getAliasInDetailsTable());
			}
		}
		return res;
	}

	public QueryInfo getQueryInfo(String connectionName, String catalog, String query) throws SQLException, IOException, StandardException, JSchException {
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

	public byte[] getBinaryData(String connectionName, String catalog, String schema, String table, String column, String idFieldName,
			String id) throws SQLException, IOException, JSchException {
		ColumnMetaData pkCol;
		if(idFieldName == null) {
	        List<ColumnMetaData> cols = metadataService.getColumns(connectionName, catalog, schema, table);
	        List<ColumnMetaData> pkCols = cols.stream().filter(ColumnMetaData::isPrimaryKey).collect(Collectors.toList());
	        if(pkCols.size() > 1) {
	            throw new IllegalArgumentException(String.format("Can not process table (%s) with more than 1 PK field", table));
	        }
	        if(pkCols.size() < 1) {
	            throw new IllegalArgumentException(String.format("Can not process table (%s) without PK field", table));
	        }
	        pkCol = pkCols.get(0);
		} else {
			pkCol = metadataService.getColumn(connectionName, catalog, schema, table, idFieldName);
		}

        Object oId = id;
        if(pkCol.getTypeId() == Types.BIGINT
                || pkCol.getTypeId() == Types.INTEGER
                || pkCol.getTypeId() == Types.NUMERIC) {
            oId = Integer.parseInt(String.valueOf(id));
        }
        String sql = "select " + column + " from " + schema + "." + table + " where " + pkCol.getName() + "=?";
        return getBinaryDataForSql(connectionName, catalog, sql, oId);
	}

	public byte[] getBinaryDataForSql(String connectionName, String catalog, String sql, Object...params) throws SQLException, IOException, JSchException {
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

	public Map<Object, String> getRowLabels(String connection, String catalog, String schema, String table, @RequestParam("ids") String[] ids) throws SQLException, IOException, JSchException {
		Map<Object, String> res = new HashMap<>();
		ColumnMetaData labelColumn = metadataService.getLabelColumn(connection, catalog, schema, table);
		ColumnMetaData pkColumn = metadataService.getPkColumn(connection, catalog, schema, table);
		boolean isMysql = dataSourceService.isMySQLDB(connection);
		String sql = isMysql ?
				String.format("select `%s` as id, `%s` as value from `%s` where " + StringUtils.repeat(String.format("`%s` = ?", pkColumn.getName()), " or ", ids.length), pkColumn.getName(), labelColumn.getName(), table, pkColumn.getName()) :
				String.format("select \"%s\" as id, \"%s\" as value from \"%s\" where " + StringUtils.repeat(String.format("\"%s\" = ?", pkColumn.getName()), " or ", ids.length), pkColumn.getName(), labelColumn.getName(), table, pkColumn.getName());

		Object[] oIds = ids;
		int[] types = new int[ids.length];
		Arrays.fill(types, pkColumn.getTypeId());
		if(MetadataService.INT_TYPES.contains(pkColumn.getTypeId())) {
			oIds = Arrays.stream(ids).map(Integer::parseInt).toArray(Integer[]::new);
		}
 		
        DataSource ds = dataSourceService.getDataSource(connection, catalog);
        JdbcTemplate template = new JdbcTemplate(ds);
        List<Map<String, Object>> lst = template.queryForList(sql, /*new RowMapper() {

			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
				res.put(rs.getObject(1), rs.getString(2));
				return null;
			}}, */ oIds, types);
		lst.forEach(m -> res.put(m.get("id"), String.valueOf(m.get("value"))));
		return res;
	}
}
