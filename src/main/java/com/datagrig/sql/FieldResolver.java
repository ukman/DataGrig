package com.datagrig.sql;

/**
 * Resolves field, i.e. identifies table name for field. Field can be
 * id
 * alias.id
 * table_name.id
 * Created by ukman on 11/2/18.
 */
public class FieldResolver {
    public FromTable resolve(SQLSelectQuery selectQuery, String field) throws SQLParseException {
        String[] parts = field.split("\\.");
        if(parts.length == 2) {
            // Contains table name
            String table = parts[0];
            String fieldName = parts[1];
            return resolve(selectQuery, table, fieldName);
        }
        if(parts.length == 1) {
            if(selectQuery.getFromTableList().size() == 1) {
                return selectQuery.getFromTableList().get(0);
            }
        }
        throw new SQLParseException("Cannot resolve '" + field + "'");
    }

    public FromTable resolve(SQLSelectQuery selectQuery, String tableOrAliasName, String field) throws SQLParseException {
        for(FromTable fromTable : selectQuery.getFromTableList()) {
            if(tableOrAliasName.equalsIgnoreCase(fromTable.getAlias())) {
                return fromTable;
            }
            if(fromTable.getAlias() == null && tableOrAliasName.equalsIgnoreCase(fromTable.getExpression())) {
                return fromTable;
            }
        }
        throw new SQLParseException("Cannot resolve '" + tableOrAliasName + "'");
    }
}
