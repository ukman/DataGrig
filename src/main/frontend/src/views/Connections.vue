<template>
  <div>
  	<div v-if="loading">
  		Loading...
  	</div>
  	<b-alert variant="danger" v-if="loadingError != null">
  	  {{ loadingError.body.message }}
  	</b-alert>
  	<b-table :items="connections">
    	<template slot="name" slot-scope="data">
    	  <router-link :to="{name:'catalogs', params:{connectionName: data.value}}">{{data.value}}</router-link>
	    </template>  	
  	</b-table>
  </div>
</template>

<script>
import config from "../config"

export default {
  name: 'connections',
  components: {
  },
  data() {return {
    	connections: [],
    	loading: true,
    	loadingError: null
  	}
  },
  created() {
  	console.log("Created");
	var resConnections = this.$resource(config.API_LOCATION + '/connections{/connectionName}');
	this.cons = resConnections.query();
	this.cons.then(response => {
		this.loading = false;
		this.connections = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}
</script>
