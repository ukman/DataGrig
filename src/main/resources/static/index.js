angular.module('DataGrigApp', ['ui.router', 'ngResource',
                               'dg.resources.ConfigConnections',
                               'dg.resources.Aliases',
                               'dg.resources.Connections',
                               'dg.controllers.alias',
                               'dg.controllers.connection', 
                               'dg.controllers.query',
                               'dg.controllers.data',
                               'dg.controllers.compare'])
.controller('DataGrigCtrl', function($scope) {
});