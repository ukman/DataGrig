<template>
  <div class="home">
    <h1>
      DataGrig
    </h1>
    <p>
      Browser for Databases
    </p>
    <p>
      <router-link :to="{name:'connections'}">Connections</router-link>
    </p>
  </div>
</template>

<script>
// @ is an alias to /src
// import HelloWorld from '@/components/HelloWorld.vue'

export default {
  name: 'home',
  components: {
  }
}
</script>
