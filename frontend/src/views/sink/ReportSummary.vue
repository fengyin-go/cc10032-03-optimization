<template>
  <div class="report-summary">
    <p v-if="loading" class="page-desc">报告加载中…</p>
    <p v-else-if="!reports.length" class="empty-state">暂无完整性报告</p>
    <table v-else class="data-table">
      <thead>
        <tr>
          <th>报告序号</th>
          <th>版本</th>
          <th>终态</th>
          <th>唯一分组</th>
          <th>成功</th>
          <th>失败</th>
          <th>成功文件</th>
          <th>重复记录</th>
          <th>报告/消费同版本</th>
          <th>完整率</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="report in reports" :key="report.report_id"
            :data-testid="`report-${report.report_seq}`">
          <td>#{{ report.report_seq }}</td>
          <td>v{{ report.version }}</td>
          <td>{{ terminalText(report.terminal_state) }}</td>
          <td>{{ report.unique_groups }}</td>
          <td>{{ report.success_groups }}</td>
          <td>{{ report.failed_groups }}</td>
          <td>{{ report.success_files }}</td>
          <td>{{ report.duplicate_records }}</td>
          <td>{{ report.consumable_matches_report ? '一致' : '不一致' }}</td>
          <td>{{ (report.integrity_rate * 100).toFixed(0) }}%</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'

import { listReports, type IntegrityReport } from '@/api/sink-recovery'

const props = defineProps<{ publicationId: string }>()
const reports = ref<IntegrityReport[]>([])
const loading = ref(true)

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

async function load() {
  loading.value = true
  try {
    const payload = await listReports(props.publicationId)
    reports.value = payload.items
  } finally {
    loading.value = false
  }
}

watch(() => props.publicationId, load, { immediate: true })
</script>
