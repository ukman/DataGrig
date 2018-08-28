angular.module('DataGrigApp').factory('Connections', function($resource){
    return $resource('/connections/:name',
        {name:'@name'},
        {
            connect: {
                method: 'POST',
                url: '/connections/:name/connect',
                params: {
                    name:'@name'
                }
            },
            catalogs: {
                method: 'GET',
                url: '/connections/:name/catalogs',
                isArray: true,
                params: {
                    name:'@name'
                }
            },
            schemas: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog',
                isArray: true,
                params: {
                    name:'@name'
                }
            },
            tables: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/:schema',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema'
                }
            },
            tableData: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/:schema/:table/data',
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table',
                    condition: '@condition'
                }
            },
            tableColumns: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/:schema/:table/columns',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table'
                }
            },
            tableDetailsForeignKeys: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/:schema/:table/detailsForeignKeys',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table'
                }
            },
            tableMasterForeignKeys: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/:schema/:table/masterForeignKeys',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table'
                }
            },
            executeQuery: {
                method: 'POST',
                url: '/connections/:name/catalogs/:catalog/execute',
                params: {
                    name: '@name',
                    catalog: '@catalog'
                }
            }

        }
    );

})
