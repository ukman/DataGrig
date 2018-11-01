<template>
    <div>
        <loading-icon v-bind:loading="loading"/>
        <b-alert variant="danger" v-if="loadingError != null">
            {{ loadingError.body.message }}
        </b-alert>
        <b-table :items="connections" :fields="connectionFields">
            <template slot="name" slot-scope="data">
                <router-link :to="{name:'catalogs', params:{connectionName: data.value}}">{{data.value}}</router-link>
            </template>
        </b-table>
    </div>
</template>

<script>
import config from "../config"
import loadingIcon from "../ui/LoadingIcon.vue";

export default {
  name: 'connections',
  components: {
    loadingIcon
  },
  data() {return {
    	connections: [],
    	loading: true,
    	loadingError: null,
    	connectionFields: [
    		{
    			key: 'name',
    		},
    		{
    			key: 'databaseProductName',
    		},
    		{
    			key: 'databaseProductVersion',
    		},
    		{
    			key: 'actions',
    		},
    	],
  	}
  },
  created() {
  	console.log("Created");
	let resConnections = this.$resource(config.API_LOCATION + '/connections{/connectionName}');
	this.cons = resConnections.query();
	this.cons.then(response => {
		this.loading = false;
		this.connections = response.data;
		this.connections.forEach(con => {
			resConnections.query({connectionName: con.name}).then(response => {
				console.log(con.name, response);
				con._rowVariant = (response.data.connected ? 'success' : 'danger');
				// TODO redesign assigning
				for(let key in response.data) {
					con[key] = response.data[key];
				}
				this.$forceUpdate();
			}, (error) => {
				console.log(con);
				con._rowVariant = 'warning';
			});
		});
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}

</script>
