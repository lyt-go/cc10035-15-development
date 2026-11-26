<template>
  <section class="batch-receipt-lab" data-testid="batch-receipt-panel" aria-label="批量导出消费封口回单">
    <div class="head">
      <div>
        <h3>封口回单消费与旧轮结果领取</h3>
        <p>批量导出只读取该轮已封口确认的实际范围；开启下一轮后，旧轮回单仍可在这里领取和导出。</p>
      </div>
      <button class="btn" type="button" data-testid="batch-receipt-refresh" @click="load">刷新可交付回单</button>
    </div>

    <p v-if="message" class="ok" role="status" data-testid="batch-receipt-message">{{ message }}</p>
    <p v-if="errorMessage" class="err" role="alert" data-testid="batch-receipt-error">{{ errorMessage }}</p>

    <div class="consume-bar">
      <label>
        <span>选择轮次/回单</span>
        <select v-model="selectedRoundId" data-testid="batch-receipt-select">
          <option v-for="round in rounds" :key="String(round.round_id)" :value="String(round.round_id)">
            {{ round.管道编号 }} · 第{{ chineseRound(round.round_no) }}轮 · {{ round.status }}
          </option>
        </select>
      </label>
      <button class="btn" type="button" data-testid="claim-final-range-batch" @click="claimSelected">
        <span data-testid="batch-claim-scope">领取本轮最终范围</span>
      </button>
      <button class="btn primary" type="button" data-testid="consume-seal-receipt" @click="exportSelected">
        <span data-testid="batch-consume-export">批量导出消费封口回单</span>
      </button>
    </div>

    <table class="data-table" data-testid="batch-receipt-table">
      <thead>
        <tr>
          <th>管道</th>
          <th>轮次</th>
          <th>回单编号</th>
          <th>状态</th>
          <th>封口实际范围</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="round in rounds" :key="String(round.round_id)" :data-testid="`batch-receipt-row-${round.round_no}`">
          <td>{{ round.管道编号 }}</td>
          <td>第{{ chineseRound(round.round_no) }}轮</td>
          <td>{{ round.receipt_no }}</td>
          <td>{{ round.status }}</td>
          <td data-testid="batch-receipt-scope">{{ (round.final_scope?.keys ?? round.accepted_keys ?? []).join('、') }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`batch-claim-${round.round_id}`" @click="claim(String(round.round_id))">
              领取最终范围
            </button>
            <button class="link" type="button" :data-testid="`batch-export-${round.round_id}`" @click="exportRound(String(round.round_id))">
              导出回单
            </button>
          </td>
        </tr>
        <tr v-if="!rounds.length">
          <td colspan="6" class="empty-state">暂无轮次回单；请先在数据流管道工作台受理记录并结束接收。</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { downloadRoundReceipt, listStreamRounds, streamRoundAction, type RoundState } from '@/api/backend-service'

const rounds = ref<RoundState[]>([])
const selectedRoundId = ref('')
const message = ref('')
const errorMessage = ref('')

const selectedRound = computed(() => rounds.value.find((r) => String(r.round_id) === selectedRoundId.value) ?? rounds.value[0])

function chineseRound(roundNo: unknown): string {
  const digits = ['一', '二', '三', '四', '五', '六', '七', '八', '九', '十']
  return digits[Number(roundNo) - 1] ?? String(roundNo ?? '')
}

async function load() {
  try {
    rounds.value = await listStreamRounds()
    if (!selectedRoundId.value && rounds.value.length) selectedRoundId.value = String(rounds.value[rounds.value.length - 1].round_id)
    errorMessage.value = ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回单列表读取失败'
  }
}

async function claim(roundId: string) {
  const result = await streamRoundAction(roundId, 'claim')
  if (!result.ok) {
    errorMessage.value = result.message
    return
  }
  message.value = result.message
  await load()
}

function claimSelected() {
  if (selectedRound.value) return claim(String(selectedRound.value.round_id))
}

async function exportRound(roundId: string) {
  try {
    await downloadRoundReceipt(roundId)
    message.value = '旧轮回单已按封口确认范围导出，并可继续领取旧轮结果'
    errorMessage.value = ''
    await load()
    window.dispatchEvent(new CustomEvent('stream-workbench-action', { detail: { moduleKey: 'batch_export', action: 'round-exported' } }))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function exportSelected() {
  if (selectedRound.value) return exportRound(String(selectedRound.value.round_id))
}

onMounted(load)
</script>

<style scoped>
.batch-receipt-lab { margin: 16px 0; padding: 18px; border: 1px solid #d8e2ef; border-radius: 14px; background: #fbfdff; }
.head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.head h3 { margin: 0 0 6px; }
.head p { margin: 0; color: #5b6b80; line-height: 1.6; }
.consume-bar { display: flex; flex-wrap: wrap; gap: 10px; align-items: end; margin: 14px 0; }
.consume-bar label { display: flex; flex-direction: column; gap: 4px; font-size: 13px; }
.consume-bar select { min-width: 320px; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 8px; }
.ok { padding: 8px 10px; border-radius: 8px; background: #ecfdf5; color: #047857; }
.err { padding: 8px 10px; border-radius: 8px; background: #fef2f2; color: #b91c1c; }
</style>
