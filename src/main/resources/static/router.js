angular.module('DataGrigApp').config(function($stateProvider, $urlRouterProvider) {

    $urlRouterProvider.otherwise('/home');

    $stateProvider

        .state('home', {
            url: '/home',
            templateUrl: 'home/home.html',
            // controller: 'ConnectionsCtrl'
        })

        .state('connections', {
            url: '/connections',
            templateUrl: 'connections/connections.html',
            controller: 'ConnectionsCtrl'
        })
        .state('editConnection', {
            url: '/connections/:name',
            templateUrl: 'connections/edit-connection.html',
            controller: 'EditConnectionCtrl'
        })
        .state('data', {
            url: '/data/:connection/:catalog/:schema/:table?condition?order?asc?limit?page',
            templateUrl: 'data/data.html',
            controller: 'DataCtrl'
        })
        .state('query', {
            url: '/query?sql',
            templateUrl: 'query/query.html',
            controller: 'QueryCtrl'
        })
        .state('alias', {
            url: '/alias/:alias/:schema/:table?condition?order?asc',
            templateUrl: 'alias/alias.html',
            controller: 'AliasCtrl'
        })
	    .state('compare', {
	        url: '/compare/:connection1/:catalog1/:schema1/with/:connection2/:catalog2/:schema2',
	        templateUrl: 'compare/compare.html',
	        controller: 'CompareCtrl'
	    });
});