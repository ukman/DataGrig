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
            sequences: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/sequences',
                isArray: true,
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema'
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
            tableRowById: {
                method: 'GET',
                url: '/connections/:name/catalogs/:catalog/schemas/:schema/tables/:table/data/:id',
                params: {
                    name:'@name',
                    catalog: '@catalog',
                    schema: '@schema',
                    table: '@table',
                    id: '@id',
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
            },
            compare: {
            	method: 'GET',
            	url: '/connections/:connection1/catalogs/:catalog1/schemas/:schema1/compareWith/:connection2/catalogs/:catalog2/schemas/:schema2',
                isArray: true,
                params: {
                	connection1: '@connection1',
                    catalog1: '@catalog1',
                    schema1: '@schema1',
                	connection2: '@connection2',
                    catalog2: '@catalog2',
                    schema2: '@schema2',
                }
            }

        }
    );

})
