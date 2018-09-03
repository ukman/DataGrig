angular.module('dg.data.ui', [])
    .controller('DataCtrl', function($scope, $stateParams, $state, Connections){
        console.log($stateParams);
        $scope.fildsFilter = '';
        delete $scope.error; 
        $scope.data = Connections.tableData({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table, condition:$stateParams.condition, order:$stateParams.order, asc:$stateParams.asc, limit:$stateParams.limit, page:$stateParams.page}, preprocessData, function(error){
        	$scope.error = error;
        });	
        console.log('Data', $scope.data);
        $scope.columns = Connections.tableColumns({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table});
        $scope.detailsForeignKeys = Connections.tableDetailsForeignKeys({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table},
            function(fks){
                fks
                    .filter(function(fk) {
                        return fk.linker
                    })
                    .forEach(function(fk){
                    	try {
                    		fk.linker = eval(fk.linker);
                    	}catch(e){
                    		console.error('Error trying to compile ' + fk.linker, e);
                    	}
                });
                console.log(fks);
            });
        $scope.masterForeignKeys = Connections.tableMasterForeignKeys({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table});

        $scope.$stateParams = $stateParams;
        $scope.condition = $stateParams.condition;

        $scope.findDetailsForeignKey = function(fieldName) {
            if(fieldName.detailsForeignKey) {
                return fieldName.detailsForeignKey;
            }
            if(this.detailsForeignKeys) {
                for(var i = 0; i < this.detailsForeignKeys.length; i++) {
                    var fk = this.detailsForeignKeys[i];
                    if(fk.fkFieldNameInDetailsTable == fieldName) {
                        fieldName.detailsForeignKey = fk;
                        return fk;
                    }
                }
            }
        }
        $scope.findMasterForeignKey = function(fieldName) {
            if(fieldName.masterForeignKeys) {
                return fieldName.masterForeignKeys;
            }
            var keys = [];
            if(this.masterForeignKeys) {
                for(var i = 0; i < this.masterForeignKeys.length; i++) {
                    var fk = this.masterForeignKeys[i];
                    if(fk.pkFieldNameInMasterTable == fieldName) {
                        keys.push(fk);
                    }
                }
            }
            fieldName.masterForeignKeys = keys;
            return keys;
        }
        $scope.loadFkInfos = function(row) {
        	row.fkInfos = Connections.tableMasterForeignKeyInfos({
        		name:$stateParams.connection, 
        		catalog:$stateParams.catalog, 
        		schema: $stateParams.schema, 
        		table:$stateParams.table,
        		id:row.id
        	});
        }
        
        $scope.sort = function(field, asc) {
            asc = asc == 'true';
            $state.go('data', {
                order: field,
                asc: !asc,
                connection: $stateParams.connection,
                catalog: $stateParams.catalog,
                schema: $stateParams.schema,
                table: $stateParams.table,
                condition: $stateParams.condition,
                limit: $stateParams.limit
            });
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
    });
