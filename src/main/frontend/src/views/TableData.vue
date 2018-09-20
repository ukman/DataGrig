<template>
    <b-container fluid>
        <b-row>
            <b-col md="12" class="my-1">
                <b-form @submit="onSubmit">
                    <b-form-group class="mb-0">
                        <b-input-group>
                            <b-form-input v-model="newCondition" placeholder="Filter Expression"/>
                            <b-input-group-append>
                                <b-btn @click="newCondition = ''">Clear</b-btn>
                                <b-button type="submit" variant="primary">Filter</b-button>
                            </b-input-group-append>
                        </b-input-group>
                    </b-form-group>
                </b-form>
            </b-col>
        </b-row>

        <b-row>
            <b-col md="6" class="my-1">
                <b-pagination-nav size="md" :number-of-pages="pagingInfo.lastPage" v-model="pagingInfo.page" :per-page="pagingInfo.limit" :link-gen="linkGen" v-if=""/>
            </b-col>
            <b-col md="6" class="my-1 text-right">
                Page size
                <router-link :to="{query: { condition: this.$route.query.condition, page: 0, limit:10, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">10 </router-link>
                <router-link :to="{query: { condition: this.$route.query.condition, page: 0, limit:20, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">20 </router-link>
                <router-link :to="{query: { condition: this.$route.query.condition, page: 0, limit:50, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">50 </router-link>
                <router-link :to="{query: { condition: this.$route.query.condition, page: 0, limit:100, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">100</router-link>
            </b-col>
        </b-row>

		<div v-if="loading">
			Loading...
		</div>
		<b-alert variant="danger" v-if="loadingError != null">
			{{ loadingError.body.message }}
		</b-alert>
		<b-table :items="tableData.data" :fields="bootStrapMetaData" caption-top small striped bordered responsive no-local-sorting tdClass="cellValue"
                 class="fixed_header data-table"
                 :sort-by.sync="sortBy"
                 :sort-desc.sync="sortDesc"
                 @sort-changed="sortingChanged"
        >
            <template :slot="'$actions'" slot-scope="row">
                <i class="fas fa-chevron-down" @click.stop="row.toggleDetails"></i>
            </template>
            <template :slot="field" slot-scope="data" v-for="(fk, field) in detailFks">
                <router-link :to="{name: 'tableData', params: {table:fk.masterTable, schema:fk.masterSchema}, query:{condition:fk.pkFieldNameInMasterTable + '=' + data.value}}">{{data.value}}</router-link>
            </template>
			<template :slot="['HEAD_', col.key].join('')" slot-scope="data" v-for="col in bootStrapMetaData">
                <div v-if="!col.action">
                    {{data.label}}
                </div>
                <div v-if="!col.action">
                    <small>
                        {{col.key}}
                    </small>
                </div>
                <div v-if="!col.action">
                    <small>
                        {{col.columnMetaData.type}}
                    </small>
                </div>
                <div v-if="detailFks[col.key]">
                    <small>
                    >>{{detailFks[col.key].masterTable}}
                    </small>
                </div>
			</template>

            <template slot="row-details" slot-scope="row">
                    <table class="table small fixed">
                        <thead>
                            <th>Property</th>
                            <th>Type</th>
                            <th>Default Value</th>
                            <th width="100%">Value</th>
                        </thead>
                        <tbody>
                            <tr v-for="col in tableMetaData">
                                <td>{{col.name}}</td>
                                <td>{{col.type}}</td>
                                <td>{{col.defaultValue}}</td>
                                <td>{{row.item[col.name]}}</td>
                            </tr>
                        </tbody>
                    </table>
            </template>

		</b-table>

    </b-container>
</template>

<script>
import config from "../config"
import moment from 'moment'

export default {
    name: 'tables',
    props: ['connectionName', 'catalog', 'schema', 'table', 'condition', 'page', 'limit'],
    components: {
    },
    data() {
        return {
            tableData: [],
            metaData: [],
            bootStrapMetaData: [],
            loading: true,
            loadingError: null,
            newCondition: this.condition,
            pagingInfo: {},
            sortBy: '',
            sortDesc: false,
            detailFks : {},
        }
    },
    computed : {
    },
    route: {
        canReuse: true,
        activate(transition) {

            console.log(transition);  // load your data
        },
        data() {
            console.log('The current ID is ' + this.$route.params.condition);
        }
    },
    watch: {
        // Route change tracking to apply URL-changes
        '$route' (to, from) {
            // react to route changes...
            this.newCondition = to.query.condition;
            this.sortBy = to.query.sortBy;
            this.sortDesc = to.query.sortDesc == 'true';
            this.page = to.query.page;
            this.limit = to.query.limit;
            this.doQuery();
            if(this.bootStrapMetaData.length == 0) {
                this.doLoadMetaData();
            }
        }
    },
    methods: {

        // Filter form submit handler
        onSubmit (evt) {
            evt.preventDefault();
            console.log(evt);
            this.$router.push({ name: 'tableData', query: { condition: this.newCondition, page: 0, limit:this.$route.query.limit, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}});
            // this.doQuery();
        },

        // Calls rest-api for getting table data
        doQuery() {
            var resConnections = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName +
                '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table +
                '/data', {condition:this.$route.query.condition, page:this.$route.query.page, limit:this.$route.query.limit,
                order:this.$route.query.sortBy,
                asc:!this.$route.query.sortDesc
                });

            var pagingConnections = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' +
                this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/paging-info',
                {condition:this.$route.query.condition, page:this.$route.query.page, limit:this.$route.query.limit});
            // this.tableData = [];
            // this.pagingInfo = {};
            resConnections.query().then(response => {
                pagingConnections.query().then(pagingInfo => {
                    console.log("pagingInfo = ", pagingInfo);
                    pagingInfo.data.page = pagingInfo.data.page + 1;
                    pagingInfo.data.lastPage = pagingInfo.data.lastPage;
                    this.pagingInfo = pagingInfo.data;
                });

                this.loading = false;
                if(response.data.data.length == 0) {
                    this.tableData = [];
                } else if(this.tableData.data && response.data.data.length == this.tableData.data.length) {
                    for(var i = 0; i < this.tableData.data.length; i++) {
                        const row = this.tableData.data[i];
                        for(var key in row) {
                            row[key] = response.data.data[i][key];
                        }
                    }
                    this.$forceUpdate();
                } else {
                    // this.tableData = response.data;//
                    // var o = {data:[response.data.data[0]]};
                    this.tableData = response.data;
                }
            },
            error => {
                this.tableData = [];
                this.loading = false;
                this.loadingError = error;
            });
        },

        // Loads metadata by calling rest-api
        doLoadMetaData() {
            var columnsConnections = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/columns');
            columnsConnections.query().then((response) => {
                console.log("MetaData = ", response);
                this.tableMetaData = response.data;
                this.bootStrapMetaData = this.metaData2VueBootStrapMetaData(response.data);
            }, error => {
                console.error(error);
            });

            const self = this;
            var detailsFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/detailsForeignKeys');
            detailsFKs.query().then((response) => {
                console.log("detailsFKs = ", response);
                response.data.forEach(fk => {
                    self.detailFks[fk.fkFieldNameInDetailsTable] = fk;
                });
                console.log("detailFks = ", self.detailFks);
            }, error => {
                console.error(error);
            });

        },

        // Converts DG metadata descriptors to Vue BottStrap column descriptors
        metaData2VueBootStrapMetaData(metaData) {
            const bsMetaData = metaData.map((column, idx, arr) => {
                const res = {
                    key : column.name,
                    tdClass: 'cell-type-' + column.type,
                    columnMetaData: column,
                    sortable: true,
                };
                if(column.type == 'date' || column.type == 'timestamp' ) {
                    res.formatter = (value, key, item) => {
                        return value ? moment(value).format(config.DATE_FORMAT) : value; // value ? "d " + new Date(value) : value;
                    };
                }
                return res;
            });
            return [{
                key: '$actions',
                tdClass: 'cell-type-action',
                sortable: false,
                action: true
            }].concat(bsMetaData);
        },

        // Generates link for paging nav
        linkGen(page) {
            return { name: 'tableData', query: { condition: this.newCondition, page: page - 1, limit:this.$route.query.limit, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}};;
        },

        // Sort changed event handler
        sortingChanged(ctx) {
            this.$router.push({ name: 'tableData', query: { condition: this.newCondition, page: 0, limit:this.$route.query.limit, sortBy:ctx.sortBy, sortDesc:ctx.sortDesc}});
        },

    },
    created() {
        this.doQuery();
        this.doLoadMetaData();
    }
}
</script>



<style>
    table.data-table>tbody>td {
        max-width:500px;
        text-overflow: ellipsis;
        overflow:hidden;
        white-space: nowrap;
    }
    td[class*="cell-type-int"],
    td[class*="cell-type-bigserial"],
    td[class*="cell-type-serial"],
    td[class*="cell-type-float"]
     {
    	text-align:right;
    }
     {
        text-align:right;
    }

</style>
