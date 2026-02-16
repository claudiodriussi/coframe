<template>
  <q-card class="library-hello-world bg-purple-1">
    <q-card-section class="row items-center q-gutter-sm">
      <q-icon
        name="menu_book"
        size="32px"
        color="purple-6"
      />
      <div class="text-h6 text-purple-8">Library Plugin Component</div>
    </q-card-section>

    <q-card-section>
      <div class="text-body1 text-purple-9 text-weight-medium q-mb-sm">
        {{ message }}
      </div>
      <div class="text-body2 text-purple-7">
        Welcome, <strong>{{ userName }}</strong>!
      </div>
    </q-card-section>

    <q-card-section>
      <q-card
        flat
        bordered
        class="bg-white"
      >
        <q-card-section>
          <div class="text-body2 text-weight-bold text-grey-8 q-mb-sm">Plugin Info:</div>
          <div class="text-caption text-grey-6">
            <ul style="padding-left: 20px">
              <li>Component: HelloWorld.vue</li>
              <li>Plugin: library</li>
              <li>Location: coframe/devtest/plugins/libapp/library/client/</li>
              <li>Registry ID: library.hello</li>
            </ul>
          </div>
        </q-card-section>
      </q-card>
    </q-card-section>

    <q-card-actions>
      <q-btn
        color="purple-6"
        label="Test Plugin Action"
        @click="handleTestAction"
      />
    </q-card-actions>
  </q-card>
</template>

<script setup lang="ts">
  import { useQuasar } from 'quasar'

  interface Props {
    message?: string
    userName?: string
  }

  const props = withDefaults(defineProps<Props>(), {
    message: 'Hello from Library Plugin!',
    userName: 'Guest',
  })

  const $q = useQuasar()

  function handleTestAction() {
    $q.notify({
      message: 'Hello from plugin component!',
      color: 'purple-6',
      icon: 'menu_book',
      position: 'top',
    })
  }
</script>

<style scoped>
  .library-hello-world {
    border: 2px solid #9c27b0;
  }
</style>
