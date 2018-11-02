package com.datagrig.controllers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.datagrig.pojo.*;
import com.datagrig.services.*;
import com.datagrig.sql.SQLParseException;
import com.datagrig.ssh.SSHKeepAlive;
import org.apache.commons.lang.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.akiban.sql.StandardException;
import com.datagrig.pojo.CompareItem.Severity;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/connections")
@Slf4j
@CrossOrigin(origins = "*")
public class ConnectionController {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private AnalyzeQueryService analyzeQueryService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private ConfigService configService;

    @Autowired
    private DataSourceService dataSourceService;

    @RequestMapping(path="", method = RequestMethod.GET)
    public List<ConnectionState> getConnections(@RequestParam(name="brief", required=false, defaultValue="true") boolean brief) {
        return connectionService.getConnections(brief);
    }

    @RequestMapping(path="/{connectionName}", method = RequestMethod.GET)
    public ConnectionState getConnection(@PathVariable("connectionName") String connectionName) throws SQLException, IOException, JSchException {
        return metadataService.getConnection(connectionName);
    }

    @RequestMapping(path="/{connectionName}/catalogs", method = RequestMethod.GET)
    public List<CatalogMetadata> getConnectionCatalogs(@PathVariable("connectionName") String connectionName) throws SQLException, IOException, JSchException {
        return metadataService.getConnectionCatalogs(connectionName);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/schemas", method = RequestMethod.GET)
    public List<SchemaMetadata> getSchemas(@PathVariable("connectionName") String connectionName,
                                           @PathVariable("catalog") String catalog) throws SQLException, IOException, JSchException {
        return metadataService.getSchemas(connectionName, catalog);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables", method = RequestMethod.GET)
    public List<TableMetadata> getTables(@PathVariable("connectionName") String connectionName,
                                         @PathVariable("catalog") String catalog,
                                         @PathVariable("schema") String schema) throws SQLException, IOException, JSchException {
        return metadataService.getTables(connectionName, catalog, schema);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/execute", method = RequestMethod.POST)
    public QueryResult executeQuery(@PathVariable("connectionName") String connectionName, @PathVariable("catalog") String catalog, @RequestBody String sql) throws SQLException, IOException, ClassNotFoundException, JSchException {
        return connectionService.executeQuery(connectionName, catalog, 0, 0, sql);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/data", method = RequestMethod.GET)
    public QueryResult getTableData(@PathVariable("connectionName") String connectionName,
                                    @PathVariable("catalog") String catalog,
                                    @PathVariable("schema") String schema,
                                    @PathVariable("table") String table,
                                    @RequestParam(name = "condition", required = false, defaultValue = "")String condition,
                                    @RequestParam(name = "limit", required = false, defaultValue = "10")int limit,
                                    @RequestParam(name = "page", required = false, defaultValue = "0")int page,
                                    @RequestParam(name = "order", required = false, defaultValue = "")String order,
                                    @RequestParam(name = "asc", required = false, defaultValue = "true")boolean asc
                                                      ) throws SQLException, IOException, StandardException, JSchException {
        QueryResult data = connectionService.getTableData(connectionName, catalog, schema, table, condition, limit, page, order, asc);
        return data;
    }
    
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/paging-info", method = RequestMethod.GET)
    public PagingInfo getTablePaginInfo(@PathVariable("connectionName") String connectionName,
    		@PathVariable("catalog") String catalog,
    		@PathVariable("schema") String schema,
    		@PathVariable("table") String table,
    		@RequestParam(name = "condition", required = false, defaultValue = "")String condition,
    		@RequestParam(name = "limit", required = false, defaultValue = "10")int limit,
    		@RequestParam(name = "page", required = false, defaultValue = "0")int page
    		) throws SQLException, IOException, StandardException, JSchException {
    	return connectionService.getTablePagingInfo(connectionName, catalog, schema, table, limit, page, condition);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/data/{id}", method = RequestMethod.GET)
    public QueryResult getTableData(@PathVariable("connectionName") String connectionName,
                                    @PathVariable("catalog") String catalog,
                                    @PathVariable("schema") String schema,
                                    @PathVariable("table") String table,
                                    @PathVariable(name = "id") String id
                                                      ) throws SQLException, IOException, StandardException, JSchException {
        return connectionService.getTableData(connectionName, catalog, schema, table, id);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns", method = RequestMethod.GET)
    public List<ColumnMetaData> getColumns(@PathVariable("connectionName") String connectionName,
                                           @PathVariable("catalog") String catalog,
                                           @PathVariable("schema") String schema,
                                           @PathVariable("table") String table) throws SQLException, IOException, JSchException {
        return metadataService.getColumns(connectionName, catalog, schema, table);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/sequences", method = RequestMethod.GET)
    public List<SequenceMetaData> getColumns(@PathVariable("connectionName") String connectionName,
                                           @PathVariable("catalog") String catalog,
                                           @PathVariable("schema") String schema) throws SQLException, IOException, JSchException {
        return metadataService.getSequences(connectionName, catalog, schema);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/detailsForeignKeys", method = RequestMethod.GET)
    public List<ForeignKeyMetaData> getDetailsForeignKeys(@PathVariable("connectionName") String connectionName,
                                                          @PathVariable("catalog") String catalog,
                                                          @PathVariable("schema") String schema,
                                                          @PathVariable("table") String table) throws SQLException, IOException, JSchException {
        List<ForeignKeyMetaData> nativeMetadata = metadataService.getDetailForeignKeys(connectionName, catalog, schema, table);
        // Add custom foreign keys
        Optional<ForeignKeyMetaData[]> customMetadata = configService.getCustomForeignKeys(connectionName);
        List<ForeignKeyMetaData> metadata = new ArrayList(nativeMetadata);
        metadata.addAll(Arrays.stream(customMetadata.orElse(new ForeignKeyMetaData[0])).filter(m -> table.equals(m.getDetailsTable())).collect(Collectors.toList()));
        return metadata;
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/masterForeignKeys", method = RequestMethod.GET)
    public List<ForeignKeyMetaData> getMasterForeignKeys(@PathVariable("connectionName") String connectionName,
                                                         @PathVariable("catalog") String catalog,
                                                         @PathVariable("schema") String schema,
                                                         @PathVariable("table") String table) throws SQLException, IOException, JSchException {
        List<ForeignKeyMetaData> nativeMetadata = metadataService.getMasterForeignKeys(connectionName, catalog, schema, table);

        // Add custom foreign keys
        Optional<ForeignKeyMetaData[]> customMetadata = configService.getCustomForeignKeys(connectionName);
        List<ForeignKeyMetaData> metadata = new ArrayList(nativeMetadata);
        metadata.addAll(Arrays.stream(customMetadata.orElse(new ForeignKeyMetaData[0])).filter(m -> table.equals(m.getMasterTable())).collect(Collectors.toList()));

        return metadata;
    }

    /**
     * Calculates count of rows in other tables that referencing row in the table with given id.
     * @param connectionName
     * @param catalog
     * @param schema
     * @param table
     * @param id
     * @return
     * @throws SQLException 
     * @throws IOException 
     * @throws ExecutionException 
     * @throws InterruptedException 
     */
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/masterForeignKeyInfos", method = RequestMethod.GET)
    public Map<String, Integer> getMasterForeignKeyInfos(@PathVariable("connectionName") String connectionName,
                                                         @PathVariable("catalog") String catalog,
                                                         @PathVariable("schema") String schema,
                                                         @PathVariable("table") String table,
                                                         @RequestParam("id") String id,
                                                         @RequestParam("path") String path) throws IOException, SQLException, InterruptedException, ExecutionException, JSchException {
        Map<String, Integer> masterInfos = connectionService.getMasterForeignKeyInfos(connectionName, catalog, schema, table, id, path);
        return connectionService.getMasterForeignKeyInfos(connectionName, catalog, schema, table, id, path);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/resolvePath", method = RequestMethod.GET)
    public List<SchemaTable> resolvePath(@PathVariable("connectionName") String connectionName,
                                   @PathVariable("catalog") String catalog,
                                   @PathVariable("schema") String schema,
                                   @PathVariable("table") String table,
                                   @RequestParam("path") String path) throws IOException, SQLException, InterruptedException, ExecutionException, JSchException {
        return connectionService.resolvePath(connectionName, catalog, schema, table, path);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/inversePath", method = RequestMethod.GET)
    public List<String> inversePath(@PathVariable("connectionName") String connectionName,
                                   @PathVariable("catalog") String catalog,
                                   @PathVariable("schema") String schema,
                                   @PathVariable("table") String table,
                                   @RequestParam("path") String path) throws IOException, SQLException, InterruptedException, ExecutionException, JSchException {
        List<String> inversePath = connectionService.inversePath(connectionName, catalog, schema, table, path);
        return inversePath;
    }

    @RequestMapping(path = "/{connectionName1}/catalogs/{catalog1}/schemas/{schema1}/compareWith/{connectionName2}/catalogs/{catalog2}/schemas/{schema2}")
    public List<CompareItem> compare(@PathVariable("connectionName1") String connectionName1,
                        @PathVariable("catalog1") String catalog1,
                        @PathVariable("schema1") String schema1,
                        @PathVariable("connectionName2") String connectionName2,
                        @PathVariable("catalog2") String catalog2,
                        @PathVariable("schema2") String schema2) throws SQLException, IOException, JSchException {
        List<CompareItem> notes = new ArrayList<>();

        List<TableMetadata> tables1 = metadataService.getTables(connectionName1, catalog1, schema1);
        List<TableMetadata> tables2 = metadataService.getTables(connectionName2, catalog2, schema2);

        Set<String> missedTables1 = connectionService.getMissed(tables1, tables2, mt -> {return ((TableMetadata)mt).getName();});
        Set<String> missedTables2 = connectionService.getMissed(tables2, tables1, mt -> {return ((TableMetadata)mt).getName();});

        notes.add(CompareItem.builder().severity(Severity.INFO).message("Missed " + missedTables2.size() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1).build());
        notes.addAll(missedTables2.stream().map(t -> CompareItem.builder().message("Missed table " + t + " in " + connectionName1 + "/" + catalog1 + "/" + schema1).severity(Severity.ERROR).build()).collect(Collectors.toList()));

        notes.add(CompareItem.builder().severity(Severity.INFO).message("Missed " + missedTables1.size() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2).build());
        notes.addAll(missedTables1.stream().map(t -> CompareItem.builder().severity(Severity.ERROR).message("Missed table " + t + " in " + connectionName2 + "/" + catalog2 + "/" + schema2).build()).collect(Collectors.toList()));

        // Check fields
        for(TableMetadata table : tables1) {
            if (!missedTables1.contains(table.getName())) {
                List<ColumnMetaData> cols1 = metadataService.getColumns(connectionName1, catalog1, schema1, table.getName());
                List<ColumnMetaData> cols2 = metadataService.getColumns(connectionName2, catalog2, schema2, table.getName());
                Set<String> missedCol1 = connectionService.getMissed(cols1, cols2, mt -> {return ((ColumnMetaData)mt).getName();});
                Set<String> missedCol2 = connectionService.getMissed(cols2, cols1, mt -> {return ((ColumnMetaData)mt).getName();});
                notes.addAll(missedCol1.stream().map(c -> CompareItem.builder().severity(Severity.ERROR).message("Missed column " + c + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName()).build()).collect(Collectors.toList()));
                notes.addAll(missedCol2.stream().map(c -> CompareItem.builder().severity(Severity.ERROR).message("Missed column " + c + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName()).build()).collect(Collectors.toList()));
                
                // Check column definition
                for(ColumnMetaData col1 : cols1) {
                	if(!missedCol1.contains(col1.getName())) {
                		ColumnMetaData col2 = cols2.stream().filter(c2 -> c2.getName().equals(col1.getName())).findFirst().orElseThrow(() -> {
                			log.info("missedCol1 = " + missedCol1);
                			log.info("missedCol2 = " + missedCol2);
                			return new IllegalStateException(String.format("Cannot find column %s", col1.getName()));});
                		
                		// Check if primary key
                		if(col1.isPrimaryKey() != col2.isPrimaryKey()) {
                			notes.add(CompareItem.builder()
                					.severity(Severity.ERROR)
                					.message("Column " + col1.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName() + (col1.isPrimaryKey() ?  " is primary key but " : " is not primary key ") +
                							"but column " + col2.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName() + (col2.isPrimaryKey() ?  " is." : " is not."))
                					.build());
                		}
                		
                		// Check if nullable
                		if(col1.isNullable() != col2.isNullable()) {
                			notes.add(CompareItem.builder()
                					.severity(Severity.ERROR)
                					.message("Column " + col1.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName() + (col1.isPrimaryKey() ?  " is nullable but " : " is not nullable ") +
                							"but column " + col2.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName() + (col2.isPrimaryKey() ?  " is." : " is not."))
                					.build());
                		}
                		
                		// Check if autoIncrement
                		if(col1.isAutoIncrement() != col2.isAutoIncrement()) {
                			notes.add(CompareItem.builder()
                					.severity(Severity.ERROR)
                					.message("Column " + col1.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName() + (col1.isPrimaryKey() ?  " is autoincremented but " : " is not autoincremented ") +
                							"but column " + col2.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName() + (col2.isPrimaryKey() ?  " is." : " is not."))
                					.build());
                		}

                		// Check data type
                		if(!col1.getType().equals(col2.getType())) {
                			Severity severity = col1.getTypeId() != col2.getTypeId() ? Severity.ERROR : Severity.WARN;
                			notes.add(CompareItem.builder()
                					.severity(severity)
                					.message("Type of column " + col1.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName() + " is " + col1.getType() + " (typeId=" + col1.getTypeId() + ", size = " + col1.getSize() + ")" +
                							" but type of column " + col2.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName() + " is " + col2.getType() + " (typeId=" + col2.getTypeId() + ", size = " + col2.getSize() + ")")
                					.build());
                			
                		}
                		
                		// Check default value
                		if(!ObjectUtils.defaultIfNull(col1.getDefaultValue(), "").equals(ObjectUtils.defaultIfNull(col2.getDefaultValue(), ""))) {
                			notes.add(CompareItem.builder()
                					.severity(Severity.ERROR)
                					.message("Default value of column " + col1.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName() + " is " + col1.getDefaultValue() +
                							" but default value of column " + col2.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName() + " is " + col2.getDefaultValue())
                					.build());
                		}
                	}
                }
            }
        }

        // Check indexes
        for(TableMetadata table : tables1) {
            if(!missedTables1.contains(table.getName())) {
                List<ForeignKeyMetaData> keys1 = metadataService.getDetailForeignKeys(connectionName1, catalog1, schema1, table.getName());
                List<ForeignKeyMetaData> keys2 = metadataService.getDetailForeignKeys(connectionName2, catalog2, schema2, table.getName());
                Set<String> missedIndexes1 = connectionService.getMissed(keys1, keys2, mt -> {return ((ForeignKeyMetaData)mt).getFkFieldNameInDetailsTable() + "->" + ((ForeignKeyMetaData)mt).getMasterTable();});
                Set<String> missedIndexes2 = connectionService.getMissed(keys2, keys1, mt -> {return ((ForeignKeyMetaData)mt).getFkFieldNameInDetailsTable() + "->" + ((ForeignKeyMetaData)mt).getMasterTable();});
                notes.addAll(missedIndexes1.stream().map(t -> CompareItem.builder().severity(Severity.ERROR).message("Missed index " + t + " in table " + table.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2).build()).collect(Collectors.toList()));
                notes.addAll(missedIndexes2.stream().map(t -> CompareItem.builder().severity(Severity.ERROR).message("Missed index " + t + " in table " + table.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1).build()).collect(Collectors.toList()));
            }
        }

        return notes;
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/queryInfos", method = RequestMethod.GET)
    public QueryInfo getQueryInfo(
    		@PathVariable("connectionName") String connectionName,
    		@PathVariable("catalog") String catalog,
    		@RequestParam("query") String query
    		) throws SQLException, IOException, StandardException, JSchException {
    	return connectionService.getQueryInfo(connectionName, catalog, query);
    }
    
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns/{column}/content-info", method = RequestMethod.GET)
    public BinaryInfo getBinaryInfo(@PathVariable("connectionName") String connectionName,
                                                        @PathVariable("catalog") String catalog,
                                                        @PathVariable("schema") String schema,
                                                        @PathVariable("table") String table,
                                                        @PathVariable("column") String column,
                                                        @RequestParam("id") String id,
                                                        @RequestParam(name="idFieldName", required=false) String idFieldName) throws IOException, SQLException, JSchException {
    	byte[] data = connectionService.getBinaryData(connectionName, catalog, schema, table, column, idFieldName, id);
    	InputStream is = new ByteArrayInputStream(data);
    	String contentType = URLConnection.guessContentTypeFromStream(is);
    	return BinaryInfo.builder().contentType(contentType).size(data.length).build();
    }
    
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns/{column}/binary", method = RequestMethod.GET)
    public void getBinaryData(@PathVariable("connectionName") String connectionName,
    		@PathVariable("catalog") String catalog,
    		@PathVariable("schema") String schema,
    		@PathVariable("table") String table,
    		@PathVariable("column") String column,
    		@RequestParam("id") String id,
    		@RequestParam(name="idFieldName", required=false) String idFieldName,
    		HttpServletResponse response) throws IOException, SQLException, JSchException {
    	byte[] data = connectionService.getBinaryData(connectionName, catalog, schema, table, column, idFieldName, id);
    	InputStream is = new ByteArrayInputStream(data);
    	String contentType = URLConnection.guessContentTypeFromStream(is);
    	response.setContentType(contentType);
    	response.getOutputStream().write(data);
    }
    
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/binary", method = RequestMethod.GET)
    public void getBinaryData(@PathVariable("connectionName") String connectionName,
                                                        @PathVariable("catalog") String catalog,
                                                        @RequestParam("sql") String sql,
                                                        @RequestParam(value="expectedContentType", required=false) String expectedContentType,
                                                        @RequestParam(value="expectedFileName", required=false) String expectedFileName,
                                                        HttpServletResponse response) throws IOException, SQLException, JSchException {
    	byte[] data = connectionService.getBinaryDataForSql(connectionName, catalog, sql);
    	if(expectedContentType == null) {
        	InputStream is = new ByteArrayInputStream(data);
        	String contentType = URLConnection.guessContentTypeFromStream(is);
    		response.setContentType(contentType);
    	} else if(expectedContentType.length() > 0) {
    		response.setContentType(expectedContentType);
    	}
    	if(expectedFileName != null) {
    		response.setHeader("Content-Disposition", "attachment; filename=" + expectedFileName);
    	}
    	response.getOutputStream().write(data);
    }
    
    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/labels")
    public Map<Object, String> getRowLabels(@PathVariable("connectionName") String connectionName,
            @PathVariable("catalog") String catalog,
            @PathVariable("schema") String schema,
            @PathVariable("table") String table,
            @RequestParam("ids") String[] ids) throws SQLException, IOException, JSchException {
    	return connectionService.getRowLabels(connectionName, catalog, schema, table, ids);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/analyze")
    public List<String> analyzeQuery(@PathVariable("connectionName") String connectionName,
            @PathVariable("catalog") String catalog,
            @RequestParam("query") String query) throws SQLException, IOException, JSchException, SQLParseException {
    	return analyzeQueryService.analyzeQuery(connectionName, catalog, query);
    }

    @RequestMapping("/ssh")
    public List<String> testJsch() throws JSchException {
    	JSch jsch=new JSch();

        
        String user = "cynteka";
        String host = "proxysp.cynteka.ru";

        Session session = jsch.getSession(user, host, 10097);

        int lport = 15432;

        String rhost = "localhost";
        int rport = 5432;
        
        UserInfo ui = new UserInfo() {
			
			@Override
			public void showMessage(String message) {
				// TODO Auto-generated method stub
				log.info(message);
			}
			
			@Override
			public boolean promptYesNo(String message) {
				log.info("promptYesNo : " + message);
				return true;
			}
			
			@Override
			public boolean promptPassword(String message) {
				return true;
			}
			
			@Override
			public boolean promptPassphrase(String message) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public String getPassword() {
				return "SalutBefog2583";
			}
			
			@Override
			public String getPassphrase() {
				return null;
			}
		};
		session.setUserInfo(ui);
		

        session.connect();

        int assinged_port = session.setPortForwardingL(0, rhost, rport);
        System.out.println("localhost:"+assinged_port+" -> "+rhost+":"+rport);

        
        List<String> res = new ArrayList();
        res.add("localhost:"+assinged_port+" -> "+rhost+":"+rport);
        return res;
    }

    @RequestMapping("/test")
    public List<String> test() throws IOException, JSchException, SQLException {
        DataSource ds = dataSourceService.getDataSource("LocalhostMySQL", "cyfoman");
        Connection con = ds.getConnection();
        DatabaseMetaData metadata = con.getMetaData();
        List<String> res = new ArrayList<>();
        ResultSet rs = metadata.getImportedKeys("cyfoman", null, "account_type");
        while(rs.next()) {
            String s = "IK " + rs.getString("FKTABLE_NAME") + " - " +
                    rs.getString("PKTABLE_NAME");
            res.add(s);
        }
        rs = metadata.getExportedKeys("cyfoman", null, "account_type");
        while(rs.next()) {
            String s = "EK " + rs.getString("FKTABLE_NAME") + " - " +
                    rs.getString("PKTABLE_NAME");
            res.add(s);
        }
        return res;
    }

    @RequestMapping("/test2")
    public List<String> test2() throws IOException, JSchException, SQLException {
        DataSource ds = dataSourceService.getDataSource("BigboxSelectelDB1", "cyn_quantech");
        Connection con = ds.getConnection();
        DatabaseMetaData metadata = con.getMetaData();
        List<String> res = new ArrayList<>();
        ResultSet rs = metadata.getImportedKeys("cyn_quantech", "public", "account_type");
        while(rs.next()) {
            String s = "2IK " + rs.getString("FKTABLE_NAME") + " - " +
                    rs.getString("PKTABLE_NAME");
            res.add(s);
        }
        rs = metadata.getExportedKeys("cyn_quantech", "public", "account_type");
        while(rs.next()) {
            String s = "2EK " + rs.getString("FKTABLE_NAME") + " - " +
                    rs.getString("PKTABLE_NAME");
            res.add(s);
        }
        return res;
    }
}
