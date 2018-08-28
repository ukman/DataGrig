angular.module('DataGrigApp')
    .controller('QueryCtrl', function($scope, $stateParams, $state, Connections){
        $scope.runQuery = function() {
            $scope.metaData = null;
            $scope.connections = Connections.query(function(connections){
                connections.forEach(function(con){
                    con.catalogs = Connections.catalogs({name:con.name}, function(catalogs){
                        catalogs.forEach(function(catalog){
                            catalog.data = Connections.executeQuery({name:con.name, catalog:catalog.name}, $scope.query, function(data){
                                if(!$scope.metaData) {
                                    $scope.metaData = data.metaData;
                                }
                                preprocessData(data);
                            }, function(error){
                                console.error(error);
                                catalog.error = error;
                            });

                        })
                    }, function(error){
                        console.error(error);
                        con.error = error;
                    });

                });


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
