<template>
    <div>
        <loading-icon v-bind:loading="loading"/>
        <b-alert variant="danger" v-if="loadingError != null">
            {{ loadingError.body.message }}
        </b-alert>
        <b-table :items="catalogs">
            <template slot="name" slot-scope="data">
                <router-link :to="{name:'schemas', params:{connectionName: connectionName, catalog: data.value}}">
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
  name: 'catalogs',
  props: ['connectionName'],
  components: {
    loadingIcon,
  },
  data() {return {
    	catalogs: [],
    	loading: true,
    	loadingError: null
  	}
  },
  created() {
  	console.log("Created ", this);
	var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.connectionName + '/catalogs');
	resConnections.query().then(response => {
		this.loading = false;
		this.catalogs = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}

</script>
