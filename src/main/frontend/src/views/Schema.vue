<template>
    <div>
        <loading-icon v-bind:loading="loading"/>
        <b-alert variant="danger" v-if="loadingError != null">
            {{ loadingError.body.message }}
        </b-alert>
        <div>
            <router-link :to="{name:'tables', params:{connectionName: connectionName, catalog: catalog, schema: schema}}">Tables</router-link>
        </div>
        <div>
            <router-link :to="{name:'sequences', params:{connectionName: connectionName, catalog: catalog, schema: schema}}">Sequences</router-link>
        </div>
    </div>
</template>

<script>
import config from "../config"
import loadingIcon from "../ui/LoadingIcon.vue";

export default {
  name: 'schema',
  props: ['connectionName', 'catalog', 'schema'],
  components: {
    loadingIcon
  },
  data() {return {
    	loading: true,
    	loadingError: null
  	}
  },
  created() {
  this.loading = false;
  /*
	var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.connectionName + '/catalogs/' + this.catalog + '/schemas/' + this.schema);
	resConnections.query().then(response => {
		this.loading = false;
		this.schema = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
*/
  }
}
</script>
