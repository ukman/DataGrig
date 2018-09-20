import Vue from 'vue'
import Router from 'vue-router'
import Connections from './views/Connections.vue'
import Catalogs from './views/Catalogs.vue'
import Schemas from './views/Schemas.vue'
import Tables from './views/Tables.vue'
import TableData from './views/TableData.vue'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/connections',
      name: 'connections',
      component: Connections
    },
    {
    	path: '/connections/:connectionName/catalogs',
    	name: 'catalogs',
    	component: Catalogs,
    	props: true
    },
    {
    	path: '/connections/:connectionName/catalogs/:catalog/schemas',
    	name: 'schemas',
    	component: Schemas,
    	props: true
    },
    {
    	path: '/connections/:connectionName/catalogs/:catalog/schemas/:schema/tables',
    	name: 'tables',
    	component: Tables,
    	props: true
    },
    {
    	path: '/connections/:connectionName/catalogs/:catalog/schemas/:schema/tables/:table/data',
    	name: 'tableData',
    	component: TableData,
    	props: (route) => (Object.assign(route.params, route.query))
    },
    {
      path: '/query',
      name: 'query',
      // route level code-splitting
      // this generates a separate chunk (about.[hash].js) for this route
      // which is lazy-loaded when the route is visited.
      component: () => import(/* webpackChunkName: "about" */ './views/About.vue')
    }
  ]
})
