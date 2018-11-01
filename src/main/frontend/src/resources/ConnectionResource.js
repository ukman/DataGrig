import Vue from 'vue';
import VueResource from 'vue-resource';

Vue.use(VueResource);

let ConnectionResource = {
    hello() {
        console.log("Hello", VueResource);
    }
};

module.exports = ConnectionResource;