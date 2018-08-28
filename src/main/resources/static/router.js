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
        .state('data', {
            url: '/data/:connection/:catalog/:schema/:table?condition?order?asc',
            templateUrl: 'data/data.html',
            controller: 'DataCtrl'
        })
        .state('query', {
            url: '/query',
            templateUrl: 'query/query.html',
            controller: 'QueryCtrl'
        })
        .state('alias', {
            url: '/alias/:alias/:schema/:table?condition?order?asc',
            templateUrl: 'alias/alias.html',
            controller: 'AliasCtrl'
        });
});