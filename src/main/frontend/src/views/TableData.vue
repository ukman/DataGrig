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

                <span v-if="pagingInfo.limit == 10">10 </span>
                <router-link v-else :to="{query: { condition: this.$route.query.condition, page: 0, limit:10, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">10 </router-link>
                <span v-if="pagingInfo.limit == 20">20 </span>
                <router-link v-else :to="{query: { condition: this.$route.query.condition, page: 0, limit:20, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">20 </router-link>
                <span v-if="pagingInfo.limit == 50">50 </span>
                <router-link v-else :to="{query: { condition: this.$route.query.condition, page: 0, limit:50, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">50 </router-link>
                <span v-if="pagingInfo.limit == 100">100 </span>
                <router-link v-else :to="{query: { condition: this.$route.query.condition, page: 0, limit:100, sortBy:this.$route.query.sortBy, sortDesc:this.$route.query.sortDesc}}">100</router-link>
            </b-col>
        </b-row>

		<b-table :items="tableData.data" :fields="bootStrapMetaData" caption-top small striped bordered responsive no-local-sorting tdClass="cell-value"
                 class="fixed_header data-table"
                 :sort-by.sync="sortBy"
                 :sort-desc.sync="sortDesc"
                 @sort-changed="sortingChanged"
        >
            <template :slot="'$actions'" slot-scope="row">
                <i class="fas fa-chevron-down" @click.stop="row.toggleDetails(); loadFkInfos(row);" title="Details" alt="Details"></i>
                <i class="fas fa-sync-alt" @click.stop="refreshRow(row)" title="Refresh" alt="Refresh"></i>
            </template>

            <template :slot="binaryColumn.name" slot-scope="data" v-for="binaryColumn in binaryColumns">
                <a :href="config.API_LOCATION + '/connections/' + $route.params.connectionName + '/catalogs/' + $route.params.catalog + '/schemas/' + $route.params.schema + '/tables/' + $route.params.table + '/columns/' + binaryColumn.name + '/binary?id=' + data.item.id + '&idFieldName=id'">
                    <span v-if="data.item[binaryColumn.name]">
                        {{data.item[binaryColumn.name].contentType}}
                        <span v-if="data.item[binaryColumn.name].size">
                            {{numeral(data.item[binaryColumn.name].size).format('0.0 b')}}
                        </span>
                    </span>
                    <span v-else @mouseover="loadBinaryType(data, binaryColumn.name)">
                        Binary
                    </span>
                </a>
            </template>

            <template :slot="arrayColumn.name" slot-scope="data" v-for="arrayColumn in arrayColumns">
                <span v-if="data.value != null">
                    [ {{ data.value.join(", ") }} ]
                </span>
            </template>

            <template :slot="field" slot-scope="data" v-for="(fk, field) in detailFks">
                <div @mouseover="loadFKTitle({name:field}, data)" class="data-type-link">
                    <router-link :to="{params:{table:fk.masterTable, schema:(fk.masterSchema ? fk.masterSchema : $route.params.schema)}, query:{condition:fk.pkFieldNameInMasterTable + '=' + data.value}}">
                        {{data.value}}
                    </router-link>
                    <span v-if="fkData[fk.fkFieldNameInDetailsTable]">
                        <small v-if="fkData[fk.fkFieldNameInDetailsTable][data.value]">
                            ({{fkData[fk.fkFieldNameInDetailsTable][data.value]}})
                        </small>

                    </span>
                </div>
            </template>
			<template :slot="['HEAD_', col.key].join('')" slot-scope="data" v-for="col in bootStrapMetaData">
                <div v-if="!col.action">
                    {{data.label}}
                </div>
                <div v-if="!col.action">
                    <small>
                        {{col.key}} :
                        {{col.columnMetaData.type}}<span v-if="col.columnMetaData.array">[]</span>
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
                            <td>{{col.type}}<span v-if="col.array">[]</span></td>
                            <td>{{col.defaultValue}}</td>
                            <td @mouseover="loadFKTitle(col, row)">
                                {{row.item[col.name]}}
                                <span v-if="fkData[col.name]">
                                    {{fkData[col.name][row.item[col.name]]}}
                                </span>
                            </td>
                        </tr>
                    </tbody>
                </table>
                <record-links-row :data="row.item" :id="row.item.id"></record-links-row>
                <!--
                <table class="table small fixed" v-if="masterFks.length > 0">
                    <thead>
                        <th>Table</th>
                        <th>Field</th>
                        <th>Connection name</th>
                        <th width="100%">Related Rows Count</th>
                    </thead>
                    <tbody>
                        <tr v-for="(mfk, idx) in masterFks" v-if="mfk.level == 0 || (row.item.$fks && row.item.$fks[fkPath(mfk.parent)])">
                            <td>
                                <div :style="'padding-left:' + mfk.level * 10 + 'px'">

                                    <span @click="expandFk(row, mfk, idx)">+</span><router-link v-if="!fkInfos[row.item[pkCol.name]] || fkInfos[row.item[pkCol.name]][mfk.name] || mfk.level > 0"
                                            :to="{name: 'tableData', params: {table:mfk.detailsTable, schema:mfk.detailsSchema}, query:{condition:generateConditionPath(mfk) + '=' + row.item[pkCol.name]}}">&nbsp;{{mfk.detailsTable}}</router-link>
                                    <span v-else>
                                        {{mfk.detailsTable}}
                                        {{mfk.level}}
                                        {{fkPath(mfk)}}
                                    </span>
                                </div>
                            </td>
                            <td>
                                {{mfk.fkFieldNameInDetailsTable}}
                            </td>
                            <td>
                                {{mfk.aliasInMasterTable}}
                            </td>
                            <td>
                                <span v-if="fkInfos[row.item[pkCol.name]]">
                                    {{fkInfos[row.item[pkCol.name]][mfk.name]}}
                                </span>
                            </td>
                        </tr>
                        <tr v-for="(dfk, idx) in detailFks" vvvvvvvv-if="dfk.level == 0 || (row.item.$fks && row.item.$fks[fkPath(dfk.parent)])">
                            <td>
                                {{dfk.masterTable}}
                            </td>
                            <td>
                                {{dfk.fkFieldNameInDetailsTable}}
                            </td>
                            <td>
                                {{dfk.aliasInDetailsTable}}
                            </td>
                        </tr>
                    </tbody>
                </table>
                -->
            </template>

		</b-table>
        <loading-icon v-bind:loading="loading"/>
        <b-alert show v-if="loadingError" variant="danger">
            {{ loadingError.body.message }}
        </b-alert>

    </b-container>
</template>

<script>
import Vue from 'vue'

import config from "../config"
import moment from 'moment'
import numeral from "numeral";
import loadingIcon from "../ui/LoadingIcon.vue";
import recordLinksRow from "../ui/RecordLinksRow.vue";

export default {
    name: 'tables',
    props: ['connectionName', 'catalog', 'schema', 'table', 'condition', 'page', 'limit'],
    components: {
        loadingIcon,
        recordLinksRow
    },
    data() {
        return {
            tableData: [],
            metaData: [],
            binaryColumns: [],
            arrayColumns: [],
            pkCol: {},
            bootStrapMetaData: [],
            loading: true,
            loadingError: null,
            newCondition: this.condition,
            pagingInfo: {},
            sortBy: '',
            sortDesc: false,
            detailFks : {},
            detailFksArr : [],
            masterFks : [],
            fks: [],
            fkData: {},
            fkInfos: {},
            config : config,
            numeral: numeral
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
            this.doLoadMetaData();
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
            this.loadingError = null;
            this.loading = true;
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
                this.prepareLinksInData();
            },
            error => {
                console.error("Error loading data", error);
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
                this.pkCol = this.tableMetaData.find(c => c.primaryKey);
                console.log("PK", this.pkCol);
                this.bootStrapMetaData = this.metaData2VueBootStrapMetaData(response.data);
                this.binaryColumns = this.tableMetaData.filter(c => c.binary);
                this.arrayColumns = this.tableMetaData.filter(c => c.array);
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
                self.detailFksArr = response.data;
                self.detailFksArr.forEach(fk => {
                    fk.schema = fk.masterSchema;
                    fk.table = fk.masterTable;
                });
                // self.prepareLinksInData();
                console.log("detailFks = ", self.detailFks);
            }, error => {
                console.error(error);
            });

            var masterFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/masterForeignKeys');
            masterFKs.query().then((response) => {
                console.log("detailsFKs = ", response);
                response.data.forEach(fk => fk.level = 0);
                self.masterFks = response.data;
                self.masterFks.forEach(fk => {
                    fk.schema = fk.detailsSchema;
                    fk.table = fk.detailsTable;
                });
                // self.prepareLinksInData();
                console.log("masterFks = ", self.masterFks);
            }, error => {
                console.error(error);
            });

        },

        // Sets links in data
        prepareLinksInData() {
            let data = this.tableData ? this.tableData.data : null;
            if(data) {
                data.forEach(item => {
                    if(!item.$links) {
                        Vue.set(item, "$links", [{
                            linkTitle:"this",
                            linkName:"",
                            schema: this.$route.params.schema,
                            table : this.$route.params.table,
                        }]);
                    }
                });
            }
            return;
            /*
            let data = this.tableData ? this.tableData.data : null;
            if(data && this.masterFks) {
                data.forEach(item => {
                    if(!item.$links) {
                        item.$links = [];
                    }
                    this.masterFks.forEach(fk => {
                        if(item.$links.indexOf(fk) < 0) {
                            item.$links.push(fk);
                        }
                    });
                });
            }
            if(data && this.detailFksArr) {
                data.forEach(item => {
                    if(!item.$links) {
                        item.$links = [];
                    }
                    this.detailFksArr.forEach(fk => {
                        if(item.$links.indexOf(fk) < 0) {
                            item.$links.push(fk);
                        }
                    });
                });
            }*/
        },

        // Converts DG metadata descriptors to Vue BottStrap column descriptors
        metaData2VueBootStrapMetaData(metaData) {
            const bsMetaData = metaData.map((column, idx, arr) => {
                const res = {
                    key : column.name,
                    tdClass: 'cell-value cell-type-' + column.type,
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

        loadFKTitle(col, row) {
            console.log(col, row);
            let needLoad = true;
            let colCache = this.fkData[col.name];
            if(colCache) {
                needLoad = !colCache[row.item[col.name]];
            }
            if(needLoad && this.detailFks[col.name]) {
                const fk = this.detailFks[col.name];
                const labels = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + fk.masterSchema + '/tables/' + fk.masterTable + '/labels');
                labels.query({ids:row.item[col.name]}).then(response => {
                    console.log(response);
                    let c = this.fkData[col.name];
                    if(!c) {
                        c = {};
                        this.fkData[col.name] = c;
                    }
                    c[row.item[col.name]] = response.data[row.item[col.name]];
                    console.log(this.fkData);
                    this.$forceUpdate();
                });
            }
        },

        refreshRow(row) {
            const id = row.item[this.pkCol.name];
            var tableRowById = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/data/' + id);
            tableRowById.query().then(response => {
                console.log(response);
                for(var key in row.item) {
                    row.item[key] = response.data.data[0][key];
                }
                this.$forceUpdate();
            });
            this.loadFkInfos(row);
        },

        loadFkInfos(row, path) {
            console.log('loadFkInfos', row);
            var loadFKInfos = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/masterForeignKeyInfos');
            loadFKInfos.query({id:row.item[this.pkCol.name]}).then((response) => {
                console.log(response);
                let newItem = {};
                newItem[row.item[this.pkCol.name]] = response.data;
                this.fkInfos = Object.assign(this.fkInfos, newItem);
                this.$forceUpdate();
            });
        },

        loadBinaryType(data, columnName) {
            console.log('loadBinaryType', data);
            data.item[columnName] = {contentType : "Loading..."};
            let loadBinaryInfo = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/columns/' + columnName + '/content-info');
            loadBinaryInfo.query({id: data.item.id, idFieldName:'id'}).then(response => {
                console.log(response);
                data.item[columnName] = response.data;
                this.$forceUpdate();
            });
            this.$forceUpdate();
        },

        generateConditionPath(fk) {
            console.log("generateConditionPath", fk);
            let res = ''; ;
            while(fk.parent) {
                res = res + fk.aliasInDetailsTable + ".";
                fk = fk.parent;
            }
            res = res + fk.fkFieldNameInDetailsTable;
            return res;
        },

        fkPath(fk) {
            let res = '';
            while(fk) {
                if(res.length > 0) {
                    res = '.' + res;
                }
                res = fk.name + res;
                fk = fk.parent;
            }
            return res;
        },

        expandFk(row, mfk, idx) {
            console.log("expandFk", mfk);
            var masterFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + mfk.detailsSchema + '/tables/' + mfk.detailsTable + '/masterForeignKeys');
            let self = this;
            mfk.level = mfk.level ? mfk.level : 0;
            masterFKs.query().then((response) => {
                console.log("detailsFKs = ", response);
                response.data.forEach(fk => {
                    fk.parent = mfk;
                    fk.level = mfk.level + 1;
                    self.masterFks.splice(idx + 1, 0, fk);
                });
                console.log("masterFks = ", self.masterFks);
                if(!row.item.$fks) {
                    row.item.$fks = {};
                }
                let path = this.fkPath(mfk);
                row.item.$fks[path] = true;
            }, error => {
                console.error(error);
            });
            var detailsFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + mfk.detailsSchema + '/tables/' + mfk.detailsTable + '/detailsForeignKeys');
            detailsFKs.query().then((response) => {
                console.log("detailsFKs = ", response);
                response.data.forEach(fk => {
                    fk.parent = mfk;
                    fk.level = mfk.level + 1;
                    self.masterFks.splice(idx + 1, 0, fk);
                });
                console.log("masterFks = ", self.masterFks);
                if(!row.item.$fks) {
                    row.item.$fks = {};
                }
                let path = this.fkPath(mfk);
                row.item.$fks[path] = true;
            }, error => {
                console.error(error);
            });
        }
    },
    created() {
        this.doQuery();
        this.doLoadMetaData();
    }
}
</script>



<style>
    td.cell-value {
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
    	width:1px;
     }
     td.cell-type-action {
    	width:10px;
     }

</style>
