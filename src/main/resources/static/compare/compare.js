angular.module('dg.controllers.compare', [])
.controller('CompareCtrl', function($scope, $stateParams, $state, Connections, ConfigConnections){
	$scope.$stateParams = $stateParams;
	$scope.compareResults = Connections.compare($stateParams, function(data){
		console.log(data);
	});
});
