angular.module('dg.controllers.connection', [])
    .controller('ConnectionsCtrl', function($scope, $stateParams, $state, Connections, ConfigConnections){
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
        $scope.deleteConnection = function(connectionName) {
        	if(confirm('Are you sure you want to delete connection ' + connectionName)) {
	        	ConfigConnections.remove({name:connectionName}, function(){
	        		for(var idx = 0; idx < $scope.connections.length; idx++) {
	        			var con = $scope.connections[idx];
	        			if(con.name == connectionName) {
	        				$scope.connections.splice(idx, 1);
	        				break;
	        			}
	        		}
	        	}, function(error){
	        		console.log(error);
	        		alert("Can not delete connection.")
	        	});
        	}
        }
    })

    .controller('EditConnectionCtrl', function($scope, $stateParams, $state, ConfigConnections){
    	if($stateParams.name && $stateParams.name.trim().length > 0) {
    		$scope.connection = ConfigConnections.get({name:$stateParams.name}, function(){
    			$scope.connection.name = $stateParams.name;
    			$scope.connection.excludeCatalogs = $scope.connection.excludeCatalogs ? $scope.connection.excludeCatalogs : []; 
    		});
    		
    	}
    	$scope.saveConnection = function() {
    		ConfigConnections.save($scope.connection, function(){
    			$state.go('editConnection', {name:$scope.connection.name})
    		});
    	}
    	$scope.testConnection = function() {
    		delete $scope.connectionOk;
    		delete $scope.connectionError;
    		$scope.testing = true;
    		$scope.testResult = ConfigConnections.testConnection($scope.connection, function(){
    			$scope.testing = false;
    			$scope.connectionOk = true;
    		}, function(error){
    			$scope.testing = false;
    			$scope.connectionOk = false;
    			console.error(error);
    			$scope.connectionError = error;
    		});
    	}
    })
;