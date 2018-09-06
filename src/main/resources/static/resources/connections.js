angular.module('dg.resources.Connections', []).factory('Connections', function($resource){
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
                url: '/connections/:name/catalogs/:catalog/schemas/:schema',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema'
                }
            },
            tableData: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/data',
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table',
                    condition: '@condition',
                    limit: '@limit',
                    page: '@page'
                }
            },
            tableColumns: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/columns',
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
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/detailsForeignKeys',
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
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/masterForeignKeys',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table'
                }
            },
            tableMasterForeignKeyInfos: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/masterForeignKeyInfos',
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
            },
            queryInfos: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/queryInfos',
                params: {
                    name: '@name',
                    catalog: '@catalog'
                }
            	
            }

        }
    );

})
