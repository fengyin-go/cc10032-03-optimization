<template>
  <div class="report-panel">
    <h3>落库完整性报告（与可消费记录同版本）</h3>
    <p class="page-desc">
      每次发布或重试后重新生成报告，分组计数只统计唯一窗口键；
      同一发布下保留不同终态的报告序列，版本号保持连续。
    </p>

    <p v-if="!publications.length" class="empty-state" data-testid="reports-empty">
      暂无数据：当前 sink 还没有发布记录，暂无完整性报告
    </p>

    <div v-for="pub in publications" :key="pub.publication_id" class="pub-block">
      <div class="pub-title">
        <strong>v{{ pub.version }} · {{ pub.source_batch }} · {{ pub.status }}</strong>
        <RouterLink class="link" :to="`/sink-recovery/report/${pub.publication_id}`">
          进入详情页
        </RouterLink>
      </div>
      <ReportSummary :publication-id="pub.publication_id" />
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Publication } from '@/api/sink-recovery'
import ReportSummary from './ReportSummary.vue'

defineProps<{ publications: Publication[] }>()
</script>

<style scoped>
.pub-block {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 14px;
  margin-bottom: 12px;
}
.pub-title {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 8px;
}
</style>
