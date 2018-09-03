angular.module('dg.connections.ui', [])
    .controller('ConnectionsCtrl', function($scope, $stateParams, $state, Connections){
        $scope.connections = Connections.query();
        $scope.connect = function(con) {
            if(!con.connected) {
                Connections.connect(con, () => {
                    con.connected = true;
                    con.catalogs = Connections.catalogs(con);
                    console.log("Catalogs", con.catalogs);
                });
            } else {
                con.catalogs = Connections.catalogs(con);
            }

        }
        $scope.toggleCatalogs = function(con) {
        	if(con.catalogs) {
        		delete con.catalogs;
        	} else {
        		con.catalogs = Connections.catalogs({name:con.name});
        	}
        }
        $scope.toggleSchemas = function(con, catalog) {
        	if(catalog.schemas) {
        		delete catalog.schemas;
        	} else {
        		catalog.schemas = Connections.schemas({name:con.name, catalog:catalog.name});
        	}
        }
        $scope.toggleTables = function(con, catalog, schema) {
        	if(schema.tables) {
        		delete schema.tables;
        	} else { 
        		schema.tables = Connections.tables({name:con.name, catalog:catalog.name, schema: schema.name});
        	}
        }
        $scope.toggleTableColumns = function(con, catalog, schema,table) {
        	if(table.columns) {
        		delete table.columns;
        		delete table.detailsForeignKey;
        		delete table.masterForeignKeys;
        	} else {
	            table.columns = Connections.tableColumns({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
	            table.detailsForeignKeys = Connections.tableDetailsForeignKeys({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
	            table.masterForeignKeys = Connections.tableMasterForeignKeys({name:con.name, catalog:catalog.name, schema: schema.name, table:table.name});
        	}
        }
    })
;