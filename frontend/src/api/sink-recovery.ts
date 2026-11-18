/** 结果落地恢复 API：窗口指标幂等落库、失败分组重试与完整性报告。 */
import { fetchJson, request } from './client'

export type WindowMetricInput = {
  window_key: string
  aggregates: Record<string, number>
  row_count: number
  watermark: number
  partition: string
  source_batch?: string
}

export type IntegrityReport = {
  report_id: string
  publication_id: string
  sink: string
  version: number
  report_seq: number
  source_batch: string
  terminal_state: string
  expected_groups: number
  unique_groups: number
  success_groups: number
  failed_groups: number
  missing_groups: string[]
  duplicate_records: number
  success_files: number
  consumable_version: number
  consumable_unique_groups: number
  consumable_matches_report: boolean
  integrity_rate: number
  complete: boolean
  retry_count: number
  error_reason: string
}

export type Publication = {
  publication_id: string
  source_batch: string
  version: number
  status: string
  retry_count: number
  frozen: boolean
  error_reason: string
  created_at: number
}

export type LandingRecord = {
  record_id: string
  publication_id: string
  sink: string
  version: number
  window_key: string
  partition: string
  aggregates: Record<string, number>
  row_count: number
  watermark: number
  source_batch: string
  write_status: string
  attempts: number
}

export type FailureGroup = {
  window_key: string
  partition: string
  attempts: number
  last_error: string
  version: number
}

type PublishOptions = {
  sink: string
  source_batch: string
  metrics: WindowMetricInput[]
  failPartitions: string[]
  interruptAfter: number | null
  transactionFail: boolean
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export function publishWindowMetrics(options: PublishOptions) {
  return postJson<{
    ok: boolean
    publication_id: string
    version: number
    mode: string
    total_groups: number
    success_groups: number
    failed_groups: number
    unique_groups: number
    terminal_state: string
    report: IntegrityReport
    note: string
  }>('/api/sink-recovery/publish', {
    sink: options.sink,
    source_batch: options.source_batch,
    metrics: options.metrics,
    fail_partitions: options.failPartitions,
    interrupt_after: options.interruptAfter,
    transaction_fail: options.transactionFail,
  })
}

export function retryFailedGroups(publicationId: string, stillFailing: string[] = []) {
  return postJson<{
    ok: boolean
    publication_id: string
    version: number
    mode: string
    success_groups: number
    failed_groups: number
    unique_groups: number
    duplicate_skipped: number
    terminal_state: string
    report: IntegrityReport
    note: string
  }>('/api/sink-recovery/retry', {
    publication_id: publicationId,
    still_failing: stillFailing,
    new_metrics: [],
  })
}

export function freezeRepublish(publicationId: string, failPartitions: string[] = []) {
  return postJson<{
    ok: boolean
    publication_id: string
    version: number
    mode: string
    success_groups: number
    failed_groups: number
    unique_groups: number
    terminal_state: string
    report: IntegrityReport
    note: string
  }>('/api/sink-recovery/republish', {
    publication_id: publicationId,
    fail_partitions: failPartitions,
  })
}

export function recoverTransaction(publicationId: string) {
  return postJson<{
    ok: boolean
    publication_id: string
    version: number
    mode: string
    success_groups: number
    failed_groups: number
    unique_groups: number
    terminal_state: string
    report: IntegrityReport
    note: string
  }>('/api/sink-recovery/recover-transaction', { publication_id: publicationId })
}

export async function listPublications(sink: string) {
  const query = encodeURIComponent(sink)
  return fetchJson<{ sink: string; total: number; items: Publication[] }>(
    `/api/sink-recovery/publications?sink=${query}`,
  )
}

export async function listConsumableRecords(sink: string) {
  const query = encodeURIComponent(sink)
  return fetchJson<{ sink: string; total: number; unique_groups: number; items: LandingRecord[] }>(
    `/api/sink-recovery/records?sink=${query}`,
  )
}

export async function listFailureGroups(publicationId: string) {
  return fetchJson<{ publication_id: string; total: number; items: FailureGroup[] }>(
    `/api/sink-recovery/failures/${publicationId}`,
  )
}

export async function listReports(publicationId: string) {
  return fetchJson<{ publication_id: string; total: number; items: IntegrityReport[] }>(
    `/api/sink-recovery/reports/${publicationId}`,
  )
}

export async function sinkSummary(sink: string) {
  const query = encodeURIComponent(sink)
  return fetchJson<{
    sink: string
    publications: number
    latest_version: number
    consumable_records: number
    unique_groups: number
    success_files: number
    open_failures: number
    latest_report: IntegrityReport | null
  }>(`/api/sink-recovery/summary?sink=${query}`)
}

export function runScenario3Demo() {
  return postJson<{
    ok: boolean
    sink: string
    publication_id: string
    concurrent_retry_modes: string[]
    unique_failure_records: number
    failure_group_keys: string[]
    summary: {
      unique_groups: number
      success_files: number
      consumable_records: number
      latest_version: number
    }
    latest_report: IntegrityReport
  }>('/api/sink-recovery/demo/scenario3', {})
}
