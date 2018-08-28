angular.module('DataGrigApp')
    .controller('AliasCtrl', function($scope, $stateParams, $state, Aliases) {
        $scope.aliasName = $stateParams.alias;
        $scope.alias = Aliases.get({name:$stateParams.alias}, function(connectionCatalog){
            console.log($scope.alias);
            console.log("Alias", connectionCatalog);
            $state.go('data', {
                order: $stateParams.order,
                asc: $stateParams.asc,
                connection: connectionCatalog.connection,
                catalog: connectionCatalog.catalog,
                schema: $stateParams.schema,
                table: $stateParams.table,
                condition: $stateParams.condition,
            });
        }, function() {
            console.log($scope.alias);
        });
    }
);