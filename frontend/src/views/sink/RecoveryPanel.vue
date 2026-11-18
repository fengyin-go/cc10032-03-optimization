<template>
  <div class="recovery-panel">
    <h3>失败分组与重试（序号3恢复入口）</h3>
    <p class="page-desc">
      重试只补齐失败窗口键并复用已成功分组；同一发布并发重试只恢复一次。
      可「逐项补齐」沿用同版本，或「冻结重发」生成连续新版本。
    </p>

    <p v-if="!publications.length" class="empty-state" data-testid="recovery-empty">
      暂无数据：当前 sink 还没有发布记录，无失败分组需要恢复
    </p>

    <div v-for="pub in publications" :key="pub.publication_id" class="pub-card"
         :data-testid="`pub-${pub.publication_id}`">
      <div class="pub-head">
        <div>
          <strong>v{{ pub.version }} · {{ pub.source_batch }}</strong>
          <span class="pub-status" :data-status="pub.status">{{ pub.status }}</span>
          <span v-if="pub.frozen" class="pub-status">已冻结</span>
        </div>
        <div class="row-actions">
          <button
            class="btn"
            type="button"
            :data-testid="`retry-${pub.publication_id}`"
            :disabled="pub.frozen || busyId === pub.publication_id"
            @click="retryInPlace(pub.publication_id)"
          >
            逐项补齐（同版本）
          </button>
          <button
            class="btn"
            type="button"
            :data-testid="`republish-${pub.publication_id}`"
            :disabled="pub.frozen || busyId === pub.publication_id"
            @click="republish(pub.publication_id)"
          >
            冻结整批重发（新版本）
          </button>
          <button
            v-if="pub.status === '已中断' && pub.error_reason.includes('事务')"
            class="btn primary"
            type="button"
            :data-testid="`recover-tx-${pub.publication_id}`"
            :disabled="busyId === pub.publication_id"
            @click="recoverTx(pub.publication_id)"
          >
            事务恢复原结果
          </button>
          <RouterLink class="btn ghost" :to="`/sink-recovery/report/${pub.publication_id}`">
            查看完整性报告
          </RouterLink>
        </div>
      </div>

      <p v-if="pub.error_reason" class="error-text" :data-testid="`reason-${pub.publication_id}`">
        异常原因：{{ pub.error_reason }}
      </p>
      <p class="page-desc">已重试 {{ pub.retry_count }} 次</p>

      <FailureGroups :publication-id="pub.publication_id" :refresh-key="refreshKey" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  freezeRepublish,
  recoverTransaction,
  retryFailedGroups,
  type Publication,
} from '@/api/sink-recovery'
import FailureGroups from './FailureGroups.vue'

const props = defineProps<{ publications: Publication[] }>()
const emit = defineEmits<{
  retryDone: []
  republishDone: []
  recoverTxDone: []
}>()

const busyId = ref('')
const refreshKey = ref(0)

async function withBusy(publicationId: string, action: () => Promise<unknown>) {
  busyId.value = publicationId
  try {
    await action()
    refreshKey.value += 1
  } finally {
    busyId.value = ''
  }
}

async function retryInPlace(publicationId: string) {
  await withBusy(publicationId, () => retryFailedGroups(publicationId))
  emit('retryDone')
}

async function republish(publicationId: string) {
  await withBusy(publicationId, () => freezeRepublish(publicationId))
  emit('republishDone')
}

async function recoverTx(publicationId: string) {
  await withBusy(publicationId, () => recoverTransaction(publicationId))
  emit('recoverTxDone')
}
</script>

<style scoped>
.pub-card {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
  margin-bottom: 12px;
}
.pub-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}
.pub-status {
  margin-left: 8px;
  font-size: 12px;
  padding: 2px 8px;
  border-radius: 10px;
  background: #eef2ff;
  color: #3730a3;
}
.pub-status[data-status='部分失败'],
.pub-status[data-status='已中断'] {
  background: #fef3c7;
  color: #92400e;
}
.pub-status[data-status='已完成'] {
  background: #dcfce7;
  color: #166534;
}
</style>
