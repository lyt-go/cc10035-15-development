<template>
  <section class="round-lab" data-testid="stream-round-panel" aria-label="管道运行轮次封口与消费回单">
    <div class="round-lab__head">
      <div>
        <h3>管道运行轮次封口与消费回单</h3>
        <p class="round-lab__hint">
          结束接收只封口当前轮：已确认受理的 A/B 不丢失；之后到达的记录自动进入后续开启的轮次。
        </p>
      </div>
      <label class="round-lab__pipeline">
        <span>选择管道</span>
        <select v-model="pipelineCode" data-testid="round-pipeline-select" @change="onPipelineChange">
          <option v-for="pipe in pipelines" :key="String(pipe.id)" :value="String(pipe['管道编号'] ?? pipe.id)">
            {{ pipe['管道编号'] ?? pipe.id }}（{{ pipe.status ?? '未知' }}）
          </option>
        </select>
      </label>
      <button class="btn" type="button" data-testid="refresh-rounds" @click="loadRounds()">刷新回单</button>
    </div>

    <p v-if="message" class="round-lab__message" data-testid="round-message" role="status">{{ message }}</p>
    <p v-if="errorMessage" class="round-lab__error" data-testid="round-error" role="alert">{{ errorMessage }}</p>

    <div class="round-lab__grid">
      <div class="round-card">
        <h4>1. 受理记录并结束接收</h4>
        <div class="round-actions">
          <label class="record-input">
            <span>记录标识</span>
            <input
              v-model="recordKey"
              name="record_key"
              aria-label="记录编号"
              data-testid="record-key-input"
              placeholder="例如 A"
              @keyup.enter="receiveKey"
            />
          </label>
          <button class="btn primary" type="button" data-testid="receive-record" @click="receiveKey">
            <span data-testid="accept-record">接收并确认受理</span>
          </button>
          <button class="btn" type="button" data-testid="receive-A" @click="receiveSpecific('A')">送达 A</button>
          <button class="btn" type="button" data-testid="receive-B" @click="receiveSpecific('B')">送达 B</button>
          <button class="btn" type="button" data-testid="receive-C" @click="receiveSpecific('C')">送达 C</button>
          <button class="btn" type="button" data-testid="receive-ab" @click="setupAB">快速受理 A、B</button>
          <button class="btn ghost" type="button" data-testid="prepare-conflict-scenario" @click="prepareScenario">布置题干冲突现场</button>
          <button class="btn warn" type="button" data-testid="finish-receiving" @click="endReceiving">
            <span data-testid="end-receiving">提交结束接收申请</span>
          </button>
          <button class="btn" type="button" data-testid="next-round" @click="openNextRound">
            <span data-testid="open-next-round">开启下一轮</span>
          </button>
        </div>
        <p class="mini-hint">停用管道可使用下方业务表中的「停用管道」；停流时会自动提交同一轮封口申请。</p>
      </div>

      <div class="round-card">
        <h4>2. 消费者恢复与幂等签收</h4>
        <div class="round-actions">
          <button class="btn" type="button" data-testid="start-b" @click="callItem('start', 'B')">B 开始处理</button>
          <button class="btn warn" type="button" data-testid="consumer-interrupt" @click="interrupt">
            <span data-testid="interrupt-consumer">消费者中断</span>
          </button>
          <button class="btn primary" type="button" data-testid="recover-consumer" @click="resume()">
            <span data-testid="resume-consumer">恢复消费者</span>
          </button>
          <label class="app-input">
            <span>原封口申请编号</span>
            <input v-model="applicationId" data-testid="application-id-input" placeholder="CLOSE-..." />
          </label>
          <button class="btn primary" type="button" data-testid="continue-original-application" @click="continueApplication">
            <span data-testid="continue-application">继续原申请</span>
          </button>
        </div>
      </div>

      <div class="round-card">
        <h4>3. 封口、领取范围与导出</h4>
        <div class="round-actions">
          <button class="btn primary" type="button" data-testid="seal-round" @click="seal">
            <span data-testid="confirm-seal">确认封口</span>
          </button>
          <button class="btn" type="button" data-testid="claim-final-range" @click="claim">
            <span data-testid="claim-scope">领取本轮最终范围</span>
          </button>
          <button class="btn" type="button" data-testid="batch-export-receipt" @click="exportCsv">
            <span data-testid="export-receipt">批量导出消费封口回单</span>
          </button>
          <button class="btn ghost" type="button" data-testid="reopen-receipt" @click="loadRounds()">重开回单</button>
        </div>
      </div>
    </div>

    <div v-if="rounds.length" class="round-tabs" data-testid="round-tabs">
      <button
        v-for="round in rounds"
        :key="String(round.round_id)"
        type="button"
        class="round-tab"
        :class="{ active: activeRoundId === round.round_id }"
        :data-testid="`round-tab-${round.round_no}`"
        @click="activeRoundId = String(round.round_id)"
      >
        第{{ chineseRound(round.round_no) }}轮 · {{ round.status }} · 成功 {{ round.success_count ?? 0 }}
      </button>
    </div>

    <div v-if="currentRound" class="round-detail" data-testid="round-detail" :data-round-id="currentRound.round_id">
      <div class="round-stats">
        <span>轮次：第{{ chineseRound(currentRound.round_no) }}轮（<strong data-testid="current-round-no" :data-round-number="currentRound.round_no">{{ currentRound.round_no }}</strong>）</span>
        <span>状态：<strong data-testid="current-round-status">{{ currentRound.status }}</strong></span>
        <span>消费者：<strong>{{ currentRound.consumer_status }}</strong></span>
        <span>已完成位置：<strong data-testid="checkpoint-key">{{ currentRound.checkpoint_key ?? '起点 0' }}</strong></span>
        <span>成功条数：<strong data-testid="success-count">{{ currentRound.success_count ?? 0 }}</strong></span>
        <span>封口申请：<strong data-testid="application-id">{{ currentRound.closure_application?.application_id ?? '未提交' }}</strong></span>
      </div>

      <table class="data-table round-items-table" data-testid="round-items-table">
        <thead>
          <tr>
            <th>位置</th>
            <th>记录</th>
            <th>受理/处理状态</th>
            <th>签收状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in currentRound.items ?? []"
            :key="String(item.item_key)"
            :data-testid="`round-item-${item.item_key}`"
            :data-item-key="item.item_key"
            :data-signed="String(Boolean(item.signed))"
          >
            <td>{{ item.seq }}</td>
            <td>{{ item.item_key }}</td>
            <td>{{ item.status }}<span v-if="item.signal_conflict" class="conflict-tag">完成信号冲突</span></td>
            <td data-testid="item-sign-status">{{ item.签收状态 ?? '未签收' }}</td>
            <td class="row-actions">
              <button
                type="button"
                class="link"
                :data-testid="`start-item-${item.item_key}`"
                @click="callItem('start', String(item.item_key))"
              >开始处理 {{ item.item_key }}</button>
              <button
                type="button"
                class="link"
                :data-testid="`interrupt-item-${item.item_key}`"
                @click="interruptItem(String(item.item_key))"
              >中断 {{ item.item_key }}</button>
              <button
                type="button"
                class="link"
                :data-testid="`sign-item-${item.item_key}`"
                @click="callItem('sign', String(item.item_key))"
              >完成并签收 {{ item.item_key }}</button>
              <button
                v-if="item.signed"
                type="button"
                class="link"
                :data-testid="`resign-item-${item.item_key}`"
                @click="callItem('sign', String(item.item_key))"
              >重新签收 {{ item.item_key }}</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="receipt-grid">
        <div class="receipt-box" data-testid="receipt">
          <div data-testid="closure-receipt" data-legacy-testid="receipt-panel" :data-receipt-no="currentRound.receipt_no">
            <h4>封口回单 {{ currentRound.receipt_no }}</h4>
            <p>受理范围（停用前已确认受理，不得消失）：
              <strong data-testid="receipt-accepted-keys">{{ (currentRound.receipt?.accepted_scope ?? currentRound.accepted_keys ?? []).join('、') || '—' }}</strong>
            </p>
            <p>实际完成范围：
              <strong data-testid="receipt-actual-keys">{{ (currentRound.receipt?.actual_scope ?? currentRound.completed_keys ?? []).join('、') || '—' }}</strong>
            </p>
            <p>封口最终范围：
              <strong data-testid="receipt-final-keys" :data-round-number="currentRound.round_no">{{ (currentRound.final_scope?.keys ?? []).join('、') || '尚未封口确认' }}</strong>
            </p>
            <p v-if="currentRound.final_scope" class="final-summary" data-testid="round-final-summary">
              第{{ chineseRound(currentRound.round_no) }}轮最终包含 {{ (currentRound.final_scope.keys ?? []).join('、') }}
            </p>
          </div>
        </div>
        <div class="receipt-box" data-testid="scope-panel">
          <h4>本轮最终范围领取</h4>
          <pre v-if="currentRound.final_scope" data-testid="final-scope">{{ JSON.stringify(currentRound.final_scope, null, 2) }}</pre>
          <p v-else>确认封口后可领取最终范围；开启下一轮后，本回单仍在此处可查、可导出。</p>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'

import {
  downloadRoundReceipt,
  listStreamRounds,
  openStreamRound,
  receiveStreamRecord,
  streamPipelineAction,
  streamRoundAction,
  type RoundState,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ pipelines: EntryRow[] }>()

const preferred = props.pipelines.find((item) => item['管道编号'] === 'DATA-0003')
  ?? props.pipelines.find((item) => item.status === '运行中')
  ?? props.pipelines[0]
const pipelineCode = ref(String(preferred?.['管道编号'] ?? preferred?.id ?? 'DATA-0003'))
const rounds = ref<RoundState[]>([])
const activeRoundId = ref('')
const recordKey = ref('')
const applicationId = ref('')
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

const currentRound = computed(() => {
  return rounds.value.find((round) => String(round.round_id) === activeRoundId.value)
    ?? rounds.value[rounds.value.length - 1]
    ?? null
})

function chineseRound(roundNo: unknown): string {
  const digits = ['一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
  const index = Number(roundNo) - 1
  return digits[index] ?? String(roundNo ?? '')
}

watch(currentRound, (round) => {
  if (round && !activeRoundId.value) activeRoundId.value = String(round.round_id)
  if (round?.closure_application?.application_id && !applicationId.value) {
    applicationId.value = String(round.closure_application.application_id)
  }
})

async function loadRounds(selectRoundId?: string) {
  errorMessage.value = ''
  try {
    rounds.value = await listStreamRounds(pipelineCode.value)
    if (selectRoundId) activeRoundId.value = selectRoundId
    if (!activeRoundId.value && rounds.value.length) {
      activeRoundId.value = String(rounds.value[rounds.value.length - 1].round_id)
    }
    if (selectRoundId === undefined && currentRound.value) {
      activeRoundId.value = String(currentRound.value.round_id)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '轮次状态读取失败'
  }
}

function onPipelineChange() {
  activeRoundId.value = ''
  applicationId.value = ''
  void loadRounds()
}

async function handleResult(result: RoundState, successFallback: string) {
  if (!result?.ok) {
    errorMessage.value = result?.message ?? '操作未完成'
    message.value = ''
    return false
  }
  message.value = result.message ?? successFallback
  errorMessage.value = ''
  const roundId = result.round?.round_id ?? result.application?.round_id ?? result.scope?.round_id
  await loadRounds(roundId ? String(roundId) : undefined)
  if (result.application?.application_id) applicationId.value = result.application.application_id
  return true
}

async function receiveKey() {
  const key = recordKey.value.trim()
  if (!key) {
    errorMessage.value = '请先填写记录标识，例如 A'
    return
  }
  busy.value = true
  try {
    const result = await receiveStreamRecord(pipelineCode.value, { item_key: key })
    await handleResult(result, '记录已受理')
    if (result.ok && !result.deferred) recordKey.value = ''
  } finally {
    busy.value = false
  }
}

async function prepareScenario() {
  const result = await streamPipelineAction(pipelineCode.value, 'prepare-conflict-scenario')
  await handleResult(result, '题干现场已布置')
}

async function setupAB() {
  const result = await receiveStreamRecord(pipelineCode.value, { keys: ['A', 'B'] })
  await handleResult(result, 'A、B 已受理')
}

async function receiveSpecific(key: string) {
  const result = await receiveStreamRecord(pipelineCode.value, { item_key: key })
  await handleResult(result, `${key} 已受理`)
}

async function endReceiving() {
  const processingKeys = (currentRound.value?.items ?? [])
    .filter((item: RoundState) => item.status === '处理中' && !item.signed)
    .map((item: RoundState) => item.item_key)
  const result = await streamPipelineAction(pipelineCode.value, 'end-receiving', {
    completion_signals: processingKeys,
  })
  await handleResult(result, '结束接收申请已提交')
}

async function openNextRound() {
  const result = await openStreamRound(pipelineCode.value)
  await handleResult(result, '下一轮已开启')
}

async function interrupt() {
  if (!currentRound.value) return
  const result = await streamRoundAction(String(currentRound.value.round_id), 'interrupt')
  await handleResult(result, '消费者已中断')
}

async function interruptItem(itemKey: string) {
  if (!currentRound.value) return
  const result = await streamRoundAction(String(currentRound.value.round_id), 'interrupt', { item_key: itemKey })
  await handleResult(result, `${itemKey} 处理已中断，进度保留`)
}

async function resume(useApplication = false) {
  if (!currentRound.value && !applicationId.value) return
  const body: Record<string, unknown> = {}
  if (useApplication && applicationId.value.trim()) body.application_id = applicationId.value.trim()
  const target = useApplication && applicationId.value.trim()
    ? applicationId.value.trim()
    : String(currentRound.value?.round_id ?? '')
  const result = await streamRoundAction(target, 'resume', body)
  await handleResult(result, '消费者已恢复')
}

function continueApplication() {
  return resume(true)
}

async function callItem(action: 'start' | 'sign', itemKey: string) {
  if (!currentRound.value) return
  const result = await streamRoundAction(String(currentRound.value.round_id), action, { item_key: itemKey })
  await handleResult(result, action === 'sign' ? '签收完成' : '处理已开始')
}

async function seal() {
  if (!currentRound.value) return
  const result = await streamRoundAction(String(currentRound.value.round_id), 'seal')
  await handleResult(result, '封口已确认')
}

async function claim() {
  if (!currentRound.value) return
  const result = await streamRoundAction(String(currentRound.value.round_id), 'claim')
  await handleResult(result, '最终范围已领取')
}

async function exportCsv() {
  if (!currentRound.value) return
  const roundId = String(currentRound.value.round_id)
  errorMessage.value = ''
  try {
    await downloadRoundReceipt(roundId)
    message.value = '封口回单已按确认范围导出，并已领取本轮最终范围'
    await loadRounds(roundId)
    window.dispatchEvent(new CustomEvent('stream-workbench-action', { detail: { moduleKey: 'batch_export', action: 'round-exported' } }))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

const onExternalAction = (event: Event) => {
  const detail = (event as CustomEvent).detail as { moduleKey?: string } | undefined
  if (!detail?.moduleKey || detail.moduleKey === 'data_stream') void loadRounds()
}

onMounted(() => {
  void loadRounds()
  window.addEventListener('stream-workbench-action', onExternalAction)
})

onUnmounted(() => {
  window.removeEventListener('stream-workbench-action', onExternalAction)
})
</script>

<style scoped>
.round-lab {
  margin: 16px 0;
  padding: 18px;
  border: 1px solid #d8e2ef;
  border-radius: 14px;
  background: linear-gradient(180deg, #f8fbff 0%, #ffffff 42%);
}
.round-lab__head { display: flex; gap: 16px; align-items: flex-start; justify-content: space-between; }
.round-lab__head h3 { margin: 0 0 6px; font-size: 18px; }
.round-lab__hint { margin: 0; color: #5b6b80; line-height: 1.6; max-width: 760px; }
.round-lab__pipeline, .record-input, .app-input { display: flex; flex-direction: column; gap: 4px; font-size: 13px; color: #44546a; }
.round-lab__pipeline select, .record-input input, .app-input input { min-width: 220px; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 8px; }
.round-lab__grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin: 14px 0; }
.round-card { padding: 12px; border: 1px solid #e2e8f0; border-radius: 12px; background: #fff; }
.round-card h4 { margin: 0 0 10px; font-size: 15px; }
.round-actions { display: flex; flex-wrap: wrap; gap: 8px; align-items: end; }
.mini-hint { width: 100%; margin: 8px 0 0; color: #718096; font-size: 12px; line-height: 1.5; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.btn.warn { border-color: #d97706; color: #9a5b00; background: #fff7ed; }
.round-lab__message { padding: 8px 10px; border-radius: 8px; background: #ecfdf5; color: #047857; }
.round-lab__error { padding: 8px 10px; border-radius: 8px; background: #fef2f2; color: #b91c1c; }
.round-tabs { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
.round-tab { padding: 7px 11px; border: 1px solid #cbd5e1; border-radius: 999px; background: #fff; cursor: pointer; }
.round-tab.active { border-color: #2563eb; background: #dbeafe; color: #1d4ed8; font-weight: 600; }
.round-detail { border-top: 1px dashed #cbd5e1; padding-top: 12px; }
.round-stats { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 10px; font-size: 14px; }
.conflict-tag { margin-left: 6px; padding: 1px 6px; border-radius: 999px; background: #fee2e2; color: #b91c1c; font-size: 12px; }
.receipt-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
.receipt-box { padding: 12px; border: 1px solid #dbeafe; border-radius: 10px; background: #f8fafc; }
.receipt-box h4 { margin-top: 0; }
.receipt-box pre { max-height: 220px; overflow: auto; white-space: pre-wrap; word-break: break-all; font-size: 12px; }
.final-summary { padding: 8px 10px; border-radius: 8px; background: #ecfdf5; color: #047857; font-weight: 600; }
@media (max-width: 900px) { .receipt-grid { grid-template-columns: 1fr; } .round-lab__head { flex-direction: column; } }
</style>
