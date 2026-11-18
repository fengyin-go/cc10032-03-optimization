<template>
  <div class="failure-groups">
    <h4>失败分组（锁定视图）</h4>
    <table v-if="groups.length" class="data-table">
      <thead>
        <tr>
          <th>窗口键</th>
          <th>分区</th>
          <th>尝试次数</th>
          <th>版本</th>
          <th>异常原因</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="group in groups" :key="group.window_key"
            :data-testid="`failure-${group.window_key}`">
          <td>{{ group.window_key }}</td>
          <td>{{ group.partition }}</td>
          <td>{{ group.attempts }}</td>
          <td>v{{ group.version }}</td>
          <td>{{ group.last_error }}</td>
        </tr>
      </tbody>
    </table>
    <p v-else class="empty-state" :data-testid="`failure-empty-${publicationId}`">
      暂无失败分组：该发布的窗口键均已成功落地，或尚未产生失败
    </p>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

import { listFailureGroups, type FailureGroup } from '@/api/sink-recovery'

const props = defineProps<{ publicationId: string; refreshKey: number }>()
const groups = ref<FailureGroup[]>([])

async function load() {
  try {
    const payload = await listFailureGroups(props.publicationId)
    groups.value = payload.items
  } catch {
    groups.value = []
  }
}

watch(() => [props.publicationId, props.refreshKey], load, { immediate: true })
</script>
