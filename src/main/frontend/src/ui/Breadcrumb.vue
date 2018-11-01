<template>
    <b-row>
        <!--
        <router-link v-for="r in currentRoute"
                :to="{name:r.name, params:$route.params}">
            {{r.title()}}
        </router-link>
        -->
        <b-col>
            <b-breadcrumb :items="items"/>
        </b-col>
    </b-row>
</template>

<script>

export default {
  name: 'breadcrumb',
  props: [],
  components: {
  },
  data() {
    return {
        currentRoute:[],
        items:[]
    }
  },
  methods: {
    updateBreadcrumb() {
        this.currentRoute = [];
        this.items = [];
        let newItems = [];
        if(this.$router && this.$router.options && this.$router.options.routes) {
            let r = this.$router.options.routes.find(r => r.name == this.$route.name);
            while(r) {
                this.currentRoute.push(r);
                let item = {text : r.title()}
                if(r.meta && r.meta.breadcrumb && r.meta.breadcrumb.title) {
                    item.text = r.meta.breadcrumb.title;
                }
                if(!(r.meta && r.meta.breadcrumb && r.meta.breadcrumb.fake)) {
                    item.to = r;
                } else {
                    item.active = true;
                }
                if(newItems.length == 0) {
                    item.active = true;
                }
                newItems.push(item);
                r = r.parent;
            }
            this.currentRoute.reverse();
            newItems.reverse();
            this.items = newItems;
        }
    }
  },
  watch: {
    '$route' (){
        this.updateBreadcrumb();
    }
  },

  created() {
    console.log("breadcrumb created");
    console.log("Route = ", this.$route);
    console.log("Router = ", this.$router);
    if(this.$router && this.$router.options && this.$router.options.routes) {
        this.$router.options.routes.forEach((child, idx) => {
            this.$router.options.routes.forEach((parent, idx) => {
                if(child != parent && child.path.indexOf(parent.path) == 0) {
                    if(child.parent) {
                        if(child.parent.path.length < parent.path.length) {
                            child.parent = parent;
                        }
                    } else {
                        child.parent = parent;
                    }
                }
            });
            let slashIdx = child.path.lastIndexOf("/");
            slashIdx = slashIdx > 0 ? slashIdx + 1 : 0;
            let lastPathItem = child.path.substring(slashIdx);
            if(lastPathItem.indexOf(":") == 0) {
                let field = lastPathItem.substring(1);
                let that = this;
                child.title = function() {
                    return that.$route.params[field];
                }
            } else {
                child.title = function(r) {
                    return child.name.substring(0, 1).toUpperCase() + child.name.substring(1);
                }
             }
            console.log("child", child);
        });
        this.updateBreadcrumb();
    }
  }
}

</script>