angular.module('dg.resources.Aliases', []).factory('Aliases', function($resource) {
    return $resource('/aliases/:name',
        {name: '@name'});
});
