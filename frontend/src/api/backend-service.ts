import { fetchJson, request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type RoundState = Record<string, any>

async function postJson<T = any>(path: string, body: Record<string, unknown> = {}): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return await response.json()
}

export async function listStreamRounds(pipelineId?: string | number): Promise<RoundState[]> {
  const query = pipelineId ? `?pipeline_id=${encodeURIComponent(String(pipelineId))}` : ''
  const payload = await fetchJson<{ items?: RoundState[]; data?: RoundState[] }>(`/api/stream-rounds${query}`)
  return payload.items ?? payload.data ?? []
}

export function receiveStreamRecord(pipelineId: string | number, body: Record<string, unknown>) {
  return postJson<RoundState>(`/api/data_stream/${encodeURIComponent(String(pipelineId))}/receive`, body)
}

export function streamPipelineAction(
  pipelineId: string | number,
  action: string,
  body: Record<string, unknown> = {},
) {
  return postJson<RoundState>(`/api/data_stream/${encodeURIComponent(String(pipelineId))}/${action}`, body)
}

export function openStreamRound(pipelineId: string | number) {
  return postJson<RoundState>(`/api/data_stream/${encodeURIComponent(String(pipelineId))}/rounds`, {})
}

export function streamRoundAction(roundId: string, action: string, body: Record<string, unknown> = {}) {
  return postJson<RoundState>(`/api/stream-rounds/${encodeURIComponent(roundId)}/${action}`, body)
}

export async function downloadRoundReceipt(roundId: string): Promise<void> {
  const response = await request(`/api/stream-rounds/${encodeURIComponent(roundId)}/export`)
  const contentType = response.headers.get('content-type') ?? ''
  if (!response.ok || contentType.includes('application/json')) {
    let message = '封口回单导出失败'
    try {
      const result = await response.json()
      message = result.message ?? message
    } catch {
      // 保留默认错误
    }
    throw new Error(message)
  }
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${roundId}-receipt.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}
