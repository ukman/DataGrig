<template>
    <b-container fluid>
        <b-row>
            <b-col md="12" class="my-1">
                <b-form>
                    <b-form-group class="mb-0">
                        <b-input-group>
                            <b-form-input v-model="filter" placeholder="Filter Tables"/>
                        </b-input-group>
                    </b-form-group>
                </b-form>
            </b-col>
        </b-row>
        <b-table :items="tables" :fields="tableFields" :filter="filter">
            <template slot="name" slot-scope="data">
                <router-link
                        :to="{name:'tableData', params:{connectionName: connectionName, catalog: catalog, schema: schema, table: data.value}}">
                    {{data.value}}
                </router-link>
            </template>
        </b-table>
        <loading-icon v-bind:loading="loading"/>
        <b-alert variant="danger" v-if="loadingError != null">
            {{ loadingError.body.message }}
        </b-alert>
    </b-container>
</template>

<script>
import config from "../config";
import numeral from "numeral";
import loadingIcon from "../ui/LoadingIcon.vue";

export default {
  name: 'tables',
  props: ['connectionName', 'catalog', 'schema'],
  components: {
    loadingIcon
  },
  data() {return {
    	tables: [],
    	loading: true,
    	loadingError: null,
    	filter: "",
    	tableFields: [
    		{
    			key: 'name',
    			sortable: true,
    		},
    		{
    			key: 'type',
    			sortable: true,
    		},
    		{
    			key: 'size',
    			sortable: true,
    			formatter: function(v){return numeral(v).format('0.0 b');},
    			tdClass: 'text-right',
    			thClass: 'text-right'
    		},
    		{
    			key: 'comment',
    			sortable: true,
    		},

    	]
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
