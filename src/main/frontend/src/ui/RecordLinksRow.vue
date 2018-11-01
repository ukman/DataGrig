<template>
    <div>
        <div v-for="link in data.$links">
            <span @click="toggle(link)">
                <i class="fas fa-chevron-down clickable" v-if="link.expanded"></i>
                <i class="fas fa-chevron-right clickable" v-else></i>
            </span>
                <router-link :to="{params:{table: link.table}, query: { condition: (makeInversePath(link).length == 0 ? 'id' : makeInversePath(link) + '.id') + '=' + id}}">
                    {{link.linkTitle}} [ {{link.table}} ]
                </router-link>

                <!--
                <b-button variant="link">
                    {{link.linkTitle}}
                </b-button>
                <span v-if="link.parent && link.parent.$linkInfos && link.parent.$linkInfos[link.name] > 0">
                    ( {{link.parent.$linkInfos[link.name]}} )

                </span>
                <span v-if="record && record[link.fkFieldNameInDetailsTable]">
                ( 1  {{link.fkFieldNameInDetailsTable}} {{link.detailsTable}} {{link.table}} {{link.linkName}})
                </span>

                    <router-link :to="{params:{table: link.table}, query: { condition: makeInversePath(link) + '.id=' + id}}">Go </router-link>
                -->

            </span>
            <div v-if="loadingMaster || loadingDetails">
                Loading...
            </div>

            <record-links-row v-if="link.expanded" :data="link" :id="id" :path="makePath(link)" style="padding-left:20px;" :record="record ? record : data">

            </record-links-row>
        </div>
    </div>
</template>

<style>
    .clickable {
        cursor:pointer;
    }
</style>

<script>
import Vue from 'vue'

import config from "../config"

export default {
    name: 'recordLinksRow',
    props: ['data', 'id', 'path', 'record'],
    components: {
    },

    data() {
        return {
            loading : {
                master:false,
                details:false,
            }

        };
    },

    methods: {
        toggle(link) {
            Vue.set(link, 'expanded', !link.expanded);
            this.loading.master = true;
            this.loading.details = true;

            const self = this;
            if(!link.$links) {
                Vue.set(link, '$links', []);
                // link.$links = [];
                var detailsFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + link.schema + '/tables/' + link.table + '/detailsForeignKeys');
                detailsFKs.query().then((response) => {
                    response.data.forEach(fk => {
                        self.loading.details = false;
                        fk.linkTitle = fk.aliasInDetailsTable;
                        fk.linkName = fk.aliasInDetailsTable;
                        fk.table = fk.masterTable;
                        fk.schema = fk.masterSchema;
                        fk.parent = link;
                        // link.$links.push(fk);
                        Vue.set(link.$links, link.$links.length, fk);
                    });
                    self.$forceUpdate();
                    console.log("size", link.$links);
                }, error => {
                    self.loading.details = false;
                    console.error(error);
                });

                var masterFKs = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + link.schema + '/tables/' + link.table + '/masterForeignKeys');
                masterFKs.query().then((response) => {
                    self.loading.master = false;
                    response.data.forEach(fk => {
                        fk.linkTitle = fk.aliasInMasterTable;
                        fk.linkName = fk.aliasInMasterTable;
                        fk.table = fk.detailsTable;
                        fk.schema = fk.detailsSchema;
                        fk.parent = link;
                        // link.$links.push(fk);
                        Vue.set(link.$links, link.$links.length, fk);
                    });
                    self.$forceUpdate();
                    console.log("size", link.$links);
                }, error => {
                    self.loading.master = false;
                    console.error(error);
                });


                var infos = this.$resource(config.API_LOCATION + '/connections/' + this.$route.params.connectionName + '/catalogs/' + this.$route.params.catalog + '/schemas/' + this.$route.params.schema + '/tables/' + this.$route.params.table + '/masterForeignKeyInfos');
                console.log(this.data);
                let path = this.path ? this.path : '';
                //*
                if(path.length > 0) {
                    path = path + '.';
                }
                path = path + link.linkName;
                //*/
                infos.query({id:this.id, path:path}).then((response) => {
                    Vue.set(link, '$linkInfos', response.data);
                    console.log('Response' ,response);
                });

            }
        },
        makePath(link) {
            let l = link;
            let res = '';
            while(l) {
                if(l.parent) {
                    if(res.length > 0) {
                        res = '.' + res
                    }
                    res = l.linkName + res;
                }
                l = l.parent;
            }
            return res;
        },

        makeInversePath(link) {
            let l = link;
            let res = '';
            while(l) {
                if(l.parent) {
                    if(res.length > 0) {
                        res = res + '.';
                    }
                    res = res + (l.linkName == l.aliasInDetailsTable ? l.aliasInMasterTable : l.aliasInDetailsTable);
                }
                l = l.parent;
            }
            return res;
        }
    },

    created() {
        console.log("RLR data = ", this.data);
    }
}

</script>