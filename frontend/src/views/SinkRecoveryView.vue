<template>
  <section class="page" data-module="sink-recovery">
    <header class="page-head">
      <div>
        <h2>结果落地 · 窗口恢复</h2>
        <p class="page-desc">
          按源端批次计算窗口指标并幂等落地：每个窗口键一次发布只产生一条成功记录，
          失败重试从失败分组继续，完整性报告与可消费记录同版本且只统计唯一分组。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="scenario3-demo" type="button" @click="runDemo3">
          演示序号3并发异常恢复
        </button>
        <RouterLink class="btn" to="/sink_output">返回结果落地配置</RouterLink>
      </div>
    </header>

    <div class="filter-bar">
      <label class="filter-item">
        <span>输出配置（sink）</span>
        <select v-model="sink" data-testid="sink-select" @change="reloadAll">
          <option v-for="name in sinkNames" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
    </div>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">发布次数</span>
        <strong class="stat-value">{{ summary.publications }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">最新连续版本</span>
        <strong class="stat-value">v{{ summary.latest_version }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">唯一分组（去重）</span>
        <strong class="stat-value" data-testid="unique-groups">{{ summary.unique_groups }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">成功文件</span>
        <strong class="stat-value" data-testid="success-files">{{ summary.success_files }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未恢复异常</span>
        <strong class="stat-value">{{ summary.open_failures }}</strong>
      </article>
    </div>

    <nav class="tab-bar">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="tab-item"
        :class="{ active: activeTab === tab.key }"
        type="button"
        :data-testid="`tab-${tab.key}`"
        @click="activeTab = tab.key"
      >
        {{ tab.label }}
      </button>
    </nav>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <!-- 发布批次 -->
    <div v-if="activeTab === 'publish'" class="tab-panel">
      <PublishPanel :sink="sink" @published="reloadAll" />
    </div>

    <!-- 唯一分组数据集（可消费记录，无半成品） -->
    <div v-else-if="activeTab === 'records'" class="tab-panel">
      <h3>唯一分组数据集（实际可消费记录）</h3>
      <p class="page-desc">
        每个窗口键仅保留最高版本的一条成功记录；冻结、中断、事务回滚的半成品不外露。
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>窗口键</th>
            <th>版本</th>
            <th>分区</th>
            <th>行数</th>
            <th>水位</th>
            <th>指标</th>
            <th>源端批次</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in records" :key="row.record_id" :data-testid="`record-${row.window_key}`">
            <td>{{ row.window_key }}</td>
            <td>v{{ row.version }}</td>
            <td>{{ row.partition }}</td>
            <td>{{ row.row_count }}</td>
            <td>{{ row.watermark }}</td>
            <td>{{ formatAggregates(row.aggregates) }}</td>
            <td>{{ row.source_batch }}</td>
          </tr>
          <tr v-if="!records.length">
            <td colspan="7" class="empty-state" data-testid="records-empty">
              暂无数据：当前 sink 还没有可消费的唯一分组记录
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 失败分组与重试 -->
    <div v-else-if="activeTab === 'recovery'" class="tab-panel">
      <RecoveryPanel
        :publications="publications"
        @retry-done="reloadAll"
        @republish-done="reloadAll"
        @recover-tx-done="reloadAll"
      />
    </div>

    <!-- 完整性报告（不同终态版本） -->
    <div v-else-if="activeTab === 'reports'" class="tab-panel">
      <ReportPanel :publications="publications" />
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  listConsumableRecords,
  listPublications,
  runScenario3Demo,
  sinkSummary,
  type IntegrityReport,
  type LandingRecord,
  type Publication,
} from '@/api/sink-recovery'
import PublishPanel from '@/views/sink/PublishPanel.vue'
import RecoveryPanel from '@/views/sink/RecoveryPanel.vue'
import ReportPanel from '@/views/sink/ReportPanel.vue'

type SinkSummary = {
  publications: number
  latest_version: number
  consumable_records: number
  unique_groups: number
  success_files: number
  open_failures: number
  latest_report: IntegrityReport | null
}

const sinkNames = ['结果落地样例1', '结果落地样例2', '结果落地样例3']
const sink = ref(sinkNames[0])
const activeTab = ref('publish')
const tabs = [
  { key: 'publish', label: '发布批次' },
  { key: 'records', label: '唯一分组数据集' },
  { key: 'recovery', label: '失败分组与重试' },
  { key: 'reports', label: '落库完整性报告' },
]

const records = ref<LandingRecord[]>([])
const publications = ref<Publication[]>([])
const summary = ref<SinkSummary>({
  publications: 0,
  latest_version: 0,
  consumable_records: 0,
  unique_groups: 0,
  success_files: 0,
  open_failures: 0,
  latest_report: null,
})
const errorMessage = ref('')

function formatAggregates(aggregates: Record<string, number>): string {
  return Object.entries(aggregates)
    .map(([key, value]) => `${key}=${value}`)
    .join('，')
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [recordPayload, publicationPayload, summaryPayload] = await Promise.all([
      listConsumableRecords(sink.value),
      listPublications(sink.value),
      sinkSummary(sink.value),
    ])
    records.value = recordPayload.items
    publications.value = publicationPayload.items
    summary.value = summaryPayload
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复数据加载失败'
  }
}

async function runDemo3() {
  errorMessage.value = ''
  try {
    const demo = await runScenario3Demo()
    sink.value = demo.sink
    activeTab.value = 'recovery'
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '序号3演示失败'
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.tab-bar {
  display: flex;
  gap: 8px;
  margin: 8px 0 16px;
  border-bottom: 1px solid var(--border);
}
.tab-item {
  border: none;
  background: none;
  padding: 8px 14px;
  cursor: pointer;
  font-size: 13px;
  color: var(--muted);
  border-bottom: 2px solid transparent;
}
.tab-item.active {
  color: var(--brand);
  border-bottom-color: var(--brand);
  font-weight: 600;
}
.tab-panel h3 {
  margin: 4px 0 8px;
  font-size: 15px;
}
.filter-item select {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
  min-width: 200px;
}
</style>
