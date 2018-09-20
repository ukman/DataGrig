import Vue from 'vue'
import VueResource from 'vue-resource'
import VueBootstrap from 'bootstrap-vue'

import App from './App.vue'
import router from './router'
import './registerServiceWorker'

Vue.config.productionTip = false
Vue.use(VueResource)
Vue.use(VueBootstrap)

import 'bootstrap/dist/css/bootstrap.css'
import 'bootstrap-vue/dist/bootstrap-vue.css'

new Vue({
  router,
  render: h => h(App)
}).$mount('#app')
