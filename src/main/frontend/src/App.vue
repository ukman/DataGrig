<template>
  <b-container fluid id="app">
    <!--
    <nav id="nav" class="nav nav-pills">
      <b-link class="nav-link" to="/connections">Connections</b-link>
      <b-link class="nav-link" to="/query">Query</b-link>
    </nav>
    -->
    <Breadcrumb/>
    <b-row>
      <b-col>
        <router-view/>
      </b-col>
    </b-row>
    <b-row>
      <!-- Footer -->
    </b-row>
  </b-container>

</template>

<style>
</style>

<script>
import Breadcrumb from "./ui/Breadcrumb.vue";

export default {
    name: 'tables',
    props: ['connectionName', 'catalog', 'schema', 'table', 'condition', 'page', 'limit'],
    components: {
      Breadcrumb
    },
    data() {
      return{};
    },
    created() {
      console.log("App created");
    }
}

</script>



