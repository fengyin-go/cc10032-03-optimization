<template>
  <div class="publish-panel">
    <h3>按源端批次发布窗口指标</h3>
    <form class="analysis-form" @submit.prevent="submit">
      <label>
        源端批次
        <input v-model="sourceBatch" data-testid="source-batch" required />
      </label>
      <label>
        窗口分组数量
        <input v-model.number="groupCount" data-testid="group-count" type="number" min="1" max="20" />
      </label>
      <label>
        失败分区（逗号分隔，留空全部成功）
        <input v-model="failPartitions" data-testid="fail-partitions" placeholder="pt_00,pt_02" />
      </label>
      <label>
        中断写入（写入N组后中断，留空不中断）
        <input v-model="interruptAfter" data-testid="interrupt-after" type="number" min="1" placeholder="如 4" />
      </label>
      <label>
        故障模式
        <select v-model="failureMode" data-testid="failure-mode">
          <option value="normal">正常写入</option>
          <option value="partition">分区写入失败</option>
          <option value="interrupt">中断写入</option>
          <option value="transaction">事务失败（整批回滚）</option>
        </select>
      </label>
      <button class="btn primary" data-testid="submit-publish" type="submit" :disabled="submitting">
        {{ submitting ? '发布中…' : '发布窗口指标' }}
      </button>
    </form>

    <div v-if="lastReport" class="report-card" data-testid="publish-result">
      <h4>发布结果 · v{{ lastReport.version }} · {{ terminalText(lastReport.terminal_state) }}</h4>
      <p class="page-desc">
        发布 ID：{{ lastPublicationId }}　成功 {{ lastReport.success_groups }} /
        失败 {{ lastReport.failed_groups }} / 唯一分组 {{ lastReport.unique_groups }}
      </p>
      <p v-if="lastReport.error_reason" class="error-text">异常原因：{{ lastReport.error_reason }}</p>
      <p v-if="lastNote" class="page-desc">{{ lastNote }}</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import { publishWindowMetrics, type IntegrityReport, type WindowMetricInput } from '@/api/sink-recovery'

const props = defineProps<{ sink: string }>()
const emit = defineEmits<{ published: [] }>()

const sourceBatch = ref(`batch_${Date.now()}`)
const groupCount = ref(6)
const failPartitions = ref('pt_00')
const interruptAfter = ref<number | null>(null)
const failureMode = ref<'normal' | 'partition' | 'interrupt' | 'transaction'>('partition')
const submitting = ref(false)
const lastReport = ref<IntegrityReport | null>(null)
const lastPublicationId = ref('')
const lastNote = ref('')

const terminalLabels: Record<string, string> = {
  writing: '写入中',
  partial_failed: '部分失败',
  completed: '已完成',
  interrupted: '已中断',
  frozen: '已冻结',
  transaction_failed: '事务失败已回滚',
  recovered: '已恢复',
}

function terminalText(state: string): string {
  return terminalLabels[state] ?? state
}

function buildMetrics(): WindowMetricInput[] {
  return Array.from({ length: groupCount.value }, (_, index) => ({
    window_key: `w_${sourceBatch.value}_${index}`,
    aggregates: { count: index + 1, sum: 10 * (index + 1) },
    row_count: index + 1,
    watermark: 1_700_000_000 + index,
    partition: `pt_${String(index % 3).padStart(2, '0')}`,
    source_batch: sourceBatch.value,
  }))
}

async function submit() {
  submitting.value = true
  lastReport.value = null
  lastNote.value = ''
  try {
    const failed = failureMode.value === 'partition'
      ? failPartitions.value.split(',').map((item) => item.trim()).filter(Boolean)
      : []
    const result = await publishWindowMetrics({
      sink: props.sink,
      source_batch: sourceBatch.value,
      metrics: buildMetrics(),
      failPartitions: failed,
      interruptAfter: failureMode.value === 'interrupt' ? (interruptAfter.value ?? 4) : null,
      transactionFail: failureMode.value === 'transaction',
    })
    lastReport.value = result.report
    lastPublicationId.value = result.publication_id
    lastNote.value = result.note
    emit('published')
  } catch (error) {
    lastNote.value = error instanceof Error ? error.message : '发布失败'
  } finally {
    submitting.value = false
  }
}
</script>

<style scoped>
.report-card {
  margin-top: 16px;
  padding: 14px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.report-card h4 {
  margin: 0 0 8px;
}
</style>
