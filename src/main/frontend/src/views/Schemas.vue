<template>
  <div>
  	<div v-if="loading">
  		Loading...
  	</div>
  	<b-alert variant="danger" v-if="loadingError != null">
  	  {{ loadingError.body.message }}
  	</b-alert>
  	<b-table :items="schemas">
    	<template slot="name" slot-scope="data">
    	  <router-link :to="{name:'tables', params:{connectionName: connectionName, catalog: catalog, schema: data.value}}">{{data.value}}</router-link>
	    </template>  	
  	</b-table>
  </div>
</template>

<script>
import config from "../config"

export default {
  name: 'schemas',
  props: ['connectionName', 'catalog'],
  components: {
  },
  data() {return {
    	schemas: [],
    	loading: true,
    	loadingError: null
  	}
  },
  created() {
  	console.log("Created ", this);
	var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.connectionName + '/catalogs/' + this.catalog + '/schemas');
	resConnections.query().then(response => {
		this.loading = false;
		this.schemas = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}
</script>
