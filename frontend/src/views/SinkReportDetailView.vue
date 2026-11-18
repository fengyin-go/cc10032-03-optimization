<template>
  <section class="page" data-module="sink-report-detail">
    <header class="page-head">
      <div>
        <h2>结果落地详情 · 序号3异常边界</h2>
        <p class="page-desc">
          串联流式源端调用链上的失败分组与落库完整性报告，
          展示同一发布在不同终态下的报告版本，接口不返回半成品结果。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/sink-recovery">返回结果落地恢复台</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="detail-error">{{ errorMessage }}</p>

    <template v-else-if="reports.length">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">发布 ID</span>
          <strong class="stat-value stat-id">{{ publicationId }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">连续版本</span>
          <strong class="stat-value">v{{ latest.version }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">终态报告序号</span>
          <strong class="stat-value">#{{ latest.report_seq }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">唯一分组 / 成功文件</span>
          <strong class="stat-value">{{ latest.unique_groups }} / {{ latest.success_files }}</strong>
        </article>
      </div>

      <div v-if="latest.error_reason" class="reason-card" data-testid="detail-reason">
        <h4>异常原因</h4>
        <p class="error-text">{{ latest.error_reason }}</p>
        <p class="page-desc">
          定位路径：流式源端（{{ latest.source_batch }}）→ 窗口聚合 → 结果落地分区写入。
        </p>
      </div>

      <h3>失败分组清单</h3>
      <table class="data-table">
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
          <tr v-for="group in failureGroups" :key="group.window_key"
              :data-testid="`detail-failure-${group.window_key}`">
            <td>{{ group.window_key }}</td>
            <td>{{ group.partition }}</td>
            <td>{{ group.attempts }}</td>
            <td>v{{ group.version }}</td>
            <td>{{ group.last_error }}</td>
          </tr>
          <tr v-if="!failureGroups.length">
            <td colspan="5" class="empty-state" data-testid="detail-failure-empty">
              暂无失败分组：全部窗口键已成功落地
            </td>
          </tr>
        </tbody>
      </table>

      <h3>完整性报告终态版本序列</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>报告序号</th>
            <th>版本</th>
            <th>终态</th>
            <th>期望分组</th>
            <th>唯一分组</th>
            <th>成功</th>
            <th>失败</th>
            <th>缺失分组</th>
            <th>报告/消费同版本</th>
            <th>完整率</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="report in reports" :key="report.report_id"
              :data-testid="`detail-report-${report.report_seq}`">
            <td>#{{ report.report_seq }}</td>
            <td>v{{ report.version }}</td>
            <td>{{ terminalText(report.terminal_state) }}</td>
            <td>{{ report.expected_groups }}</td>
            <td>{{ report.unique_groups }}</td>
            <td>{{ report.success_groups }}</td>
            <td>{{ report.failed_groups }}</td>
            <td>{{ report.missing_groups.join('、') || '—' }}</td>
            <td>{{ report.consumable_matches_report ? '一致' : '不一致' }}</td>
            <td>{{ (report.integrity_rate * 100).toFixed(0) }}%</td>
          </tr>
        </tbody>
      </table>
    </template>

    <div v-else class="empty-state" data-testid="detail-empty">
      暂无数据：该发布不存在或尚未生成完整性报告
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import {
  listFailureGroups,
  listReports,
  type FailureGroup,
  type IntegrityReport,
} from '@/api/sink-recovery'

const route = useRoute()
const publicationId = String(route.params.publicationId)
const reports = ref<IntegrityReport[]>([])
const failureGroups = ref<FailureGroup[]>([])
const errorMessage = ref('')

const terminalLabels: Record<string, string> = {
  writing: '写入中',
  partial_failed: '部分失败',
  completed: '已完成',
  interrupted: '已中断',
  frozen: '已冻结',
  transaction_failed: '事务失败已回滚',
  recovered: '已恢复',
}

const latest = computed(() =>
  reports.value.length
    ? reports.value.reduce((a, b) => (a.report_seq > b.report_seq ? a : b))
    : ({} as IntegrityReport),
)

function terminalText(state: string): string {
  return terminalLabels[state] ?? state
}

onMounted(async () => {
  try {
    const [reportPayload, failurePayload] = await Promise.all([
      listReports(publicationId),
      listFailureGroups(publicationId),
    ])
    reports.value = reportPayload.items
    failureGroups.value = failurePayload.items
    if (!reportPayload.items) {
      errorMessage.value = '该发布暂无完整性报告'
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '详情加载失败'
  }
})
</script>

<style scoped>
.stat-id {
  font-size: 13px;
}
.reason-card {
  background: #fffbeb;
  border: 1px solid #fde68a;
  border-radius: 8px;
  padding: 12px 14px;
  margin-bottom: 16px;
}
.reason-card h4 {
  margin: 0 0 6px;
}
h3 {
  margin: 18px 0 8px;
  font-size: 15px;
}
</style>
