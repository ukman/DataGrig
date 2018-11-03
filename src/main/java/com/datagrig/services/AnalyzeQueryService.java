package com.datagrig.services;

import com.datagrig.sql.*;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ukman on 11/2/18.
 */
@Service
@Slf4j
public class AnalyzeQueryService {

    @Autowired
    private DataSourceService dataSourceService;

    public List<String> analyzeQuery(String connectionName, String catalog, String query) throws IOException, JSchException, SQLParseException, SQLException {
        SQLParser parser = new SQLParser();
        SQLSelectQuery sqlQuery = (SQLSelectQuery) parser.parse(query);
        DataSource ds = dataSourceService.getDataSource(connectionName, catalog);
        List<String> res = new ArrayList<>();
        res.add("Analyze '" + query + "'");

        SQLSimpleConditionParser simpleConditionParser = new SQLSimpleConditionParser();
        FieldResolver fieldResolver = new FieldResolver();
        
        try (Connection con = ds.getConnection()) {
            DatabaseMetaData metadata = con.getMetaData();
            for (FromTable fromTable : sqlQuery.getFromTableList()) {
                res.add(fromTable.getExpression() + " as " + fromTable.getAlias() + " on " + fromTable.getOnExpression());
                if(fromTable.getOnExpression() != null) {
                    try {
                        SQLSimpleCondition simpleCondition = simpleConditionParser.parse(fromTable.getOnExpression());
                        FromTable leftTable = fieldResolver.resolve(sqlQuery, simpleCondition.getLeft());
                        FromTable rightTable = fieldResolver.resolve(sqlQuery, simpleCondition.getRight());
                        checkIndex(res, metadata, fromTable.getExpression(), simpleCondition.getLeft(), leftTable, catalog);
                        checkIndex(res, metadata, fromTable.getExpression(), simpleCondition.getRight(), rightTable, catalog);

                        res.add("");
                    } catch (Exception e) {
                        log.error("Error parsing " + fromTable.getOnExpression(), e);
                    }
                }
            }
        }
        return res;
    }


    public List<String> analyzeQuery(String query) throws IOException, JSchException, SQLParseException, SQLException {
        SQLParser parser = new SQLParser();
        SQLSelectQuery sqlQuery = (SQLSelectQuery) parser.parse(query);
        List<String> res = new ArrayList<>();
        res.add("Analyze '" + query + "'");

        SQLSimpleConditionParser simpleConditionParser = new SQLSimpleConditionParser();
        FieldResolver fieldResolver = new FieldResolver();

        for (FromTable fromTable : sqlQuery.getFromTableList()) {
            res.add("Check index for table " + fromTable);
        }
        return res;
    }

    private void checkIndex(List<String> res, DatabaseMetaData metadata, String expression, String tableField, FromTable table, String catalog) {
        try {
            ResultSet indexRs = metadata.getIndexInfo(catalog, null, table.getExpression(), false, false);
            boolean found = false;
            String indexName = null;
            while(indexRs.next() && !found) {
                String column = indexRs.getString("COLUMN_NAME");
                indexName = indexRs.getString("INDEX_NAME") + " " + indexRs.getString("TABLE_SCHEM") + "." + indexRs.getString("TABLE_NAME");
                if(column.equalsIgnoreCase(tableField) || tableField.toLowerCase().endsWith("." + column.toLowerCase())) {
                    found = true;
                    break;
                }
            }
            if(!found) {
                res.add("Cannot find index for field " + tableField + " from table " + table.getOnExpression());
            } else {
                res.add("Found index " + indexName + " for field " + tableField + " from table " + table.getExpression());

            }
        }catch (Exception e) {
            log.error("", e);
        }
    }
}
