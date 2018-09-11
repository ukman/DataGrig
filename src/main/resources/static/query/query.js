angular.module('dg.controllers.query', ['dg.utils'])
    .controller('QueryCtrl', function($scope, $stateParams, $state, Connections, DGUtils){
    	$scope.query = $stateParams.sql;
        $scope.runQuery = function(sql) {
            $scope.metaData = null;
            $scope.total={};
            $scope.connections = Connections.query(function(connections){
                connections.forEach(function(con){
                    con.catalogs = Connections.catalogs({name:con.name}, function(catalogs){
                        catalogs.forEach(function(catalog){
                            catalog.data = Connections.executeQuery({name:con.name, catalog:catalog.name}, sql, function(data){
                                if(!$scope.metaData) {
                                    $scope.metaData = data.metaData;
                                }
                                DGUtils.preprocessData(data);
                            }, function(error){
                                console.error(error);
                                catalog.error = error;
                            });
                            catalog.queryInfos = Connections.queryInfos({name:con.name, catalog:catalog.name, query: $scope.query});

                        })
                    }, function(error){
                        console.error(error);
                        con.error = error;
                    });

                });


            });
            $scope.detailsForeignKeys = Connections.tableDetailsForeignKeys({name:$stateParams.connection, catalog:$stateParams.catalog, schema: $stateParams.schema, table:$stateParams.table},
                    function(fks){
                        fks
                            .filter(function(fk) {
                                return fk.linker
                            })
                            .forEach(function(fk){
                            fk.linker = eval(fk.linker);
                        });
                        // console.log(fks);
                    });
            $scope.findDetailsForeignKey = function(fieldName, queryInfos) {
                if(fieldName.detailsForeignKey) {
                    return fieldName.detailsForeignKey;
                }
                if(queryInfos && queryInfos.detailForeignKeys) {
                    for(var i = 0; i < queryInfos.detailForeignKeys.length; i++) {
                        var fk = queryInfos.detailForeignKeys[i];
                        if(fk.fkFieldNameInDetailsTable == fieldName) {
                            fieldName.detailsForeignKey = fk;
                            return fk;
                        }
                    }
                }
            }
            

        }
        if($stateParams.sql && $stateParams.sql.length > 0) {
        	$scope.runQuery($stateParams.sql)
        }
        function preprocessData(data) {
            for(var i = 0; i < data.data.length; i++) {
                var row = data.data[i];
                for(var j = 0; j < data.metaData.length; j++) {
                    var field = data.metaData[j];
                    var value = row[field.name];
                    if(field.type == 'timestamp' || field.type == 'date') {
                        if(Number.isInteger(value)) {
                            row[field.name] = new Date(value);
                        }
                    }
                    if(Number.isInteger(value) || Number.isFloat(value)) {
	                    var sum = $scope.total[field.name];
	                    sum = sum ? sum : 0;
	                    sum += value;
	                    $scope.total[field.name] = sum;
                    }
                }
            }
        }

    });
