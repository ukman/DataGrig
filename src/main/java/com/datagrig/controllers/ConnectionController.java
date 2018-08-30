package com.datagrig.controllers;

import com.akiban.sql.StandardException;
import com.datagrig.pojo.*;
import com.datagrig.services.ConfigService;
import com.datagrig.services.ConnectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/connections")
public class ConnectionController {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConfigService configService;

    @RequestMapping(path="", method = RequestMethod.GET)
    public List<ConnectionState> getConnections() {
        return connectionService.getConnections();
    }

    @RequestMapping(path="/{connectionName}/catalogs", method = RequestMethod.GET)
    public List<CatalogMetadata> getConnectionCatalogs(@PathVariable("connectionName") String connectionName) throws SQLException, IOException {
        return connectionService.getConnectionCatalogs(connectionName);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}", method = RequestMethod.GET)
    public List<SchemaMetadata> getSchemas(@PathVariable("connectionName") String connectionName,
                                           @PathVariable("catalog") String catalog) throws SQLException, IOException {
        return connectionService.getSchemas(connectionName, catalog);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/schemas/{schema}", method = RequestMethod.GET)
    public List<TableMetadata> getTables(@PathVariable("connectionName") String connectionName,
                                         @PathVariable("catalog") String catalog,
                                         @PathVariable("schema") String schema) throws SQLException, IOException {
        return connectionService.getTables(connectionName, catalog, schema);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/connect", method = RequestMethod.POST)
    public void connect(@PathVariable("connectionName") String connectionName, @PathVariable("catalog") String catalog) throws SQLException, IOException, ClassNotFoundException {
        connectionService.connect(connectionName, catalog);
    }

    @RequestMapping(path="/{connectionName}/catalogs/{catalog}/disconnect", method = RequestMethod.POST)
    public void disconnect(@PathVariable("connectionName") String connectionName, @PathVariable("catalog") String catalog) throws SQLException, IOException, ClassNotFoundException {
        connectionService.disconnect(connectionName, catalog);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/execute", method = RequestMethod.POST)
    public QueryResult executeQuery(@PathVariable("connectionName") String connectionName, @PathVariable("catalog") String catalog, @RequestBody String sql) throws SQLException, IOException, ClassNotFoundException {
        return connectionService.executeQuery(connectionName, catalog, sql);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/data", method = RequestMethod.GET)
    public QueryResult getTableData(@PathVariable("connectionName") String connectionName,
                                    @PathVariable("catalog") String catalog,
                                    @PathVariable("schema") String schema,
                                    @PathVariable("table") String table,
                                    @RequestParam(name = "condition", required = false, defaultValue = "")String condition,
                                    @RequestParam(name = "order", required = false, defaultValue = "")String order,
                                    @RequestParam(name = "asc", required = false, defaultValue = "true")boolean asc
                                                      ) throws SQLException, IOException {
        return connectionService.getTableData(connectionName, catalog, schema, table, condition, order, asc);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns", method = RequestMethod.GET)
    public List<ColumnMetaData> getColumns(@PathVariable("connectionName") String connectionName,
                                           @PathVariable("catalog") String catalog,
                                           @PathVariable("schema") String schema,
                                           @PathVariable("table") String table) throws SQLException, IOException {
        return connectionService.getColumns(connectionName, catalog, schema, table);
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/detailsForeignKeys", method = RequestMethod.GET)
    public List<ForeignKeyMetaData> getDetailsForeignKeys(@PathVariable("connectionName") String connectionName,
                                                          @PathVariable("catalog") String catalog,
                                                          @PathVariable("schema") String schema,
                                                          @PathVariable("table") String table) throws SQLException, IOException {
        List<ForeignKeyMetaData> nativeMetadata = connectionService.getDetailForeignKeys(connectionName, catalog, schema, table);
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
                                                         @PathVariable("table") String table) throws SQLException, IOException {
        List<ForeignKeyMetaData> nativeMetadata = connectionService.getMasterForeignKeys(connectionName, catalog, schema, table);

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
     */
    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/schemas/{schema}/tables/{table}/masterForeignKeyInfos", method = RequestMethod.GET)
    public Map<String, Integer> getMasterForeignKeyInfos(@PathVariable("connectionName") String connectionName,
                                                        @PathVariable("catalog") String catalog,
                                                        @PathVariable("schema") String schema,
                                                        @PathVariable("table") String table,
                                                        @RequestParam("id") String id) throws IOException, SQLException {
        return connectionService.getMasterForeignKeyInfos(connectionName, catalog, schema, table, id);
    }

    @RequestMapping(path = "/{connectionName1}/catalogs/{catalog1}/schemas/{schema1}/compareWith/{connectionName2}/catalogs/{catalog2}/schemas/{schema2}")
    public List<String> compare(@PathVariable("connectionName1") String connectionName1,
                        @PathVariable("catalog1") String catalog1,
                        @PathVariable("schema1") String schema1,
                        @PathVariable("connectionName2") String connectionName2,
                        @PathVariable("catalog2") String catalog2,
                        @PathVariable("schema2") String schema2) throws SQLException, IOException {
        List<String> notes = new ArrayList<>();

        List<TableMetadata> tables1 = connectionService.getTables(connectionName1, catalog1, schema1);
        List<TableMetadata> tables2 = connectionService.getTables(connectionName2, catalog2, schema2);

        Set<String> missedTables1 = connectionService.getMissed(tables1, tables2, mt -> {return ((TableMetadata)mt).getName();});
        Set<String> missedTables2 = connectionService.getMissed(tables2, tables1, mt -> {return ((TableMetadata)mt).getName();});

        notes.add("Missed " + missedTables2.size() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1);
        notes.addAll(missedTables2.stream().map(t -> "Missed table " + t + " in " + connectionName1 + "/" + catalog1 + "/" + schema1).collect(Collectors.toList()));

        notes.add("Missed " + missedTables1.size() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2);
        notes.addAll(missedTables1.stream().map(t -> "Missed table " + t + " in " + connectionName2 + "/" + catalog2 + "/" + schema2).collect(Collectors.toList()));

        // Check fields
        for(TableMetadata table : tables1) {
            if (!missedTables1.contains(table.getName())) {
                List<ColumnMetaData> cols1 = connectionService.getColumns(connectionName1, catalog1, schema1, table.getName());
                List<ColumnMetaData> cols2 = connectionService.getColumns(connectionName2, catalog2, schema2, table.getName());
                Set<String> missedCol1 = connectionService.getMissed(cols1, cols2, mt -> {return ((ColumnMetaData)mt).getName();});
                Set<String> missedCol2 = connectionService.getMissed(cols2, cols1, mt -> {return ((ColumnMetaData)mt).getName();});
                notes.addAll(missedCol1.stream().map(c -> "Missed column " + c + " in " + connectionName2 + "/" + catalog2 + "/" + schema2 + "/" + table.getName()).collect(Collectors.toList()));
                notes.addAll(missedCol2.stream().map(c -> "Missed column " + c + " in " + connectionName1 + "/" + catalog1 + "/" + schema1 + "/" + table.getName()).collect(Collectors.toList()));
            }
        }

        // Check indexes
        for(TableMetadata table : tables1) {
            if(!missedTables1.contains(table.getName())) {
                List<ForeignKeyMetaData> keys1 = connectionService.getDetailForeignKeys(connectionName1, catalog1, schema1, table.getName());
                List<ForeignKeyMetaData> keys2 = connectionService.getDetailForeignKeys(connectionName2, catalog2, schema2, table.getName());
                Set<String> missedIndexes1 = connectionService.getMissed(keys1, keys2, mt -> {return ((ForeignKeyMetaData)mt).getFkFieldNameInDetailsTable() + "->" + ((ForeignKeyMetaData)mt).getMasterTable();});
                Set<String> missedIndexes2 = connectionService.getMissed(keys2, keys1, mt -> {return ((ForeignKeyMetaData)mt).getFkFieldNameInDetailsTable() + "->" + ((ForeignKeyMetaData)mt).getMasterTable();});
                notes.addAll(missedIndexes1.stream().map(t -> "Missed index " + t + " in table " + table.getName() + " in " + connectionName2 + "/" + catalog2 + "/" + schema2).collect(Collectors.toList()));
                notes.addAll(missedIndexes2.stream().map(t -> "Missed index " + t + " in table " + table.getName() + " in " + connectionName1 + "/" + catalog1 + "/" + schema1).collect(Collectors.toList()));
            }
        }

        return notes;
    }

    @RequestMapping(path = "/{connectionName}/catalogs/{catalog}/queryInfos", method = RequestMethod.GET)
    public QueryInfo getQueryInfo(
    		@PathVariable("connectionName") String connectionName,
    		@PathVariable("catalog") String catalog,
    		@RequestParam("query") String query
    		) throws SQLException, IOException, StandardException {
    	return connectionService.getQueryInfo(connectionName, catalog, query);
    }
}
