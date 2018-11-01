<template>
    <div v-if="loading" class="text-center">
        <ball-scale-ripple-multiple-loader color="#BBB" size="100px"></ball-scale-ripple-multiple-loader>
    </div>
</template>

<script>

export default {
  name: 'loading-icon',
  props: ['loading'], // :'Boolean'},
  components: {
  },
  data() {
    return {
    }
  },
  created() {
    console.log("LoadingIcon created");
  }
}
</script>
