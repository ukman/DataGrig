angular.module('DataGrigApp').factory('Aliases', function($resource) {
    return $resource('/aliases/:name',
        {name: '@name'});
});
