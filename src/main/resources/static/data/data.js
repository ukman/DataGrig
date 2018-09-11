angular.module('dg.controllers.data', ['dg.utils'])
    .controller('DataCtrl', function($scope, $stateParams, $state, Connections, DGUtils){
        console.log($stateParams);
        $scope.fildsFilter = '';
        delete $scope.error; 
        $scope.data = Connections.tableData({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table, condition:$stateParams.condition, order:$stateParams.order, asc:$stateParams.asc, limit:$stateParams.limit, page:$stateParams.page}, DGUtils.preprocessData, function(error){
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
        
        $scope.refresh = function(row, id) {
        	var idx = this.data.data.indexOf(row);
        	console.log(idx, this.data.data[idx]);
        	row.$loading = true;
        	delete row.$error
        	Connections.tableRowById({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table, id:id}, function(data){
        		DGUtils.preprocessData(data);
        		console.log(data);
        		if(data.data.length == 1) {
        			$scope.data.data[idx] = data.data[0];
        		} else {
        			$scope.data.data.splice(idx, 1);
        		}
        	}, function(error) {
        		row.$loading = false;
        		console.error(error);
        		row.$error = error;
        	});
        }
    });
