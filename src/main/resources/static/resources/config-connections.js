angular.module('dg.resources.ConfigConnections', []).factory('ConfigConnections', function($resource){
    return $resource('/config/connections/:name',
        {name:'@name'},
        {
            testConnection: {
                method: 'PUT',
                url: '/config/connections/:name/test',
                params: {
                    name:'@name'
                }
            },
        }
    );

})
