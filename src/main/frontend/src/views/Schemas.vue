<template>
    <div>
        <loading-icon v-bind:loading="loading"/>
        <b-alert variant="danger" v-if="loadingError != null">
            {{ loadingError.body.message }}
        </b-alert>
        <b-table :items="schemas">
            <template slot="name" slot-scope="data">
                <router-link
                        :to="{name:'schema', params:{connectionName: connectionName, catalog: catalog, schema: data.value}}">
                    {{data.value}}
                </router-link>
            </template>
        </b-table>
    </div>
</template>

<script>
import config from "../config"
import loadingIcon from "../ui/LoadingIcon.vue";

export default {
  name: 'schemas',
  props: ['connectionName', 'catalog'],
  components: {
    loadingIcon
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
