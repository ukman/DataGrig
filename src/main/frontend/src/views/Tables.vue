<template>
  <div>
  	<div v-if="loading">
  		Loading...
  	</div>
  	<b-alert variant="danger" v-if="loadingError != null">
  	  {{ loadingError.body.message }}
  	</b-alert>
  	<b-table :items="tables">
    	<template slot="name" slot-scope="data">
    	  <router-link :to="{name:'tableData', params:{connectionName: connectionName, catalog: catalog, schema: schema, table: data.value}}">{{data.value}}</router-link>
	    </template>  	
  	</b-table>
  </div>
</template>

<script>
import config from "../config"

export default {
  name: 'tables',
  props: ['connectionName', 'catalog', 'schema'],
  components: {
  },
  data() {return {
    	tables: [],
    	loading: true,
    	loadingError: null
  	}
  },
  created() {
  	console.log("Created ", this);
	var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.connectionName + '/catalogs/' + this.catalog + '/schemas/' + this.schema + '/tables');
	resConnections.query().then(response => {
		this.loading = false;
		this.tables = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}
</script>
