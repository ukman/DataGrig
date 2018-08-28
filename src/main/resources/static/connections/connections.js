angular.module('DataGrigApp')
    .controller('ConnectionsCtrl', function($scope, $stateParams, $state, Connections){
        $scope.connections = Connections.query();
        $scope.connect = function(con) {
            if(!con.connected) {
                Connections.connect(con, () => {
                    con.connected = true;
                    con.catalogs = Connections.catalogs(con);
                });
            } else {
                con.catalogs = Connections.catalogs(con);
            }

        }
        $scope.getCatalogs = function(con) {
            con.catalogs = Connections.catalogs({name:con.name});
        }
        $scope.getSchemas = function(con, catalog) {
            catalog.schemas = Connections.schemas({name:con.name, catalog:catalog.name});
        }
        $scope.getTables = function(con, catalog, schema) {
            schema.tables = Connections.tables({name:con.name, catalog:catalog.name, schema: schema.name});
        }
        $scope.getTableData = function(con, catalog, schema,table) {
            table.data = Connections.tableData({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name}, function(data){
                preprocessData(data)
            });
            table.columns = Connections.tableColumns({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
            table.detailsForeignKeys = Connections.tableDetailsForeignKeys({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
            table.masterForeignKeys = Connections.tableMasterForeignKeys({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
        }

        $scope.findDetailsForeignKey = function(table, fieldName) {
            if(table.detailsForeignKeys) {
                for(var i = 0; i < table.detailsForeignKeys.length; i++) {
                    var fk = table.detailsForeignKeys[i];
                    if(fk.fkFieldNameInDetailsTable == fieldName) {
                        return fk;
                    }
                }
            }
        }

        function preprocessData(data) {
            for(var i = 0; i < data.data.length; i++) {
                var row = data.data[i];
                for(var j = 0; j < data.metaData.length; j++) {
                    var field = data.metaData[j];
                    if(field.type == 'timestamp' || field.type == 'date') {
                        var value = row[field.name];
                        if(Number.isInteger(value)) {
                            row[field.name] = new Date(value);
                        }
                    }
                }
            }
        }
    })
;