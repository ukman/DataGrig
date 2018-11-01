<template>
    <b-container fluid>
        <b-row>
            <b-col md="12" class="my-1">
                <b-form>
                    <b-form-group class="mb-0">
                        <b-input-group>
                            <b-form-input v-model="filter" placeholder="Filter Sequences"/>
                        </b-input-group>
                    </b-form-group>
                </b-form>
            </b-col>
        </b-row>
        <b-table :items="sequences" :fields="sequenceFields" :filter="filter">
            <!--
            <template slot="name" slot-scope="data">
              <router-link :to="{name:'tableData', params:{connectionName: connectionName, catalog: catalog, schema: schema, table: data.value}}">{{data.value}}</router-link>
            </template>
            -->
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
    	sequences: [],
    	loading: true,
    	loadingError: null,
    	filter: "",
    	sequenceFields: [
    		{
    			key: 'name',
    			sortable: true,
    		},
    		{
    			key: 'value',
    			sortable: true,
    		},
    		{
    			key: 'minValue',
    			sortable: true,
    		},
    		{
    			key: 'maxValue',
    			sortable: true,
    		},
    		{
    			key: 'increment',
    			sortable: true,
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
	var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.connectionName + '/catalogs/' + this.catalog + '/schemas/' + this.schema + '/sequences');
	resConnections.query().then(response => {
		this.loading = false;
		this.sequences = response.data;
	},
	error => {
		this.loading = false;
		this.loadingError = error;
	});
  	
  }
}

</script>
