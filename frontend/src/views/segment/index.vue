<template>
  <section class="page" data-module="segment">
    <header class="page-head">
      <div>
        <h2>分群分析管理</h2>
        <p class="page-desc">定义用户或业务对象的分群规则，支持条件筛选、规模控制和周期性刷新。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记人群分群</button>
        <button class="btn" type="button" @click="runBatch">执行周期刷新</button>
        <button class="btn" type="button" @click="exportRows">导出分群分析清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p v-if="infoMessage" class="info-text">{{ infoMessage }}</p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">
            <span v-if="column === 'status'" class="tag" :class="{ danger: row.abnormal }">
              {{ row.abnormal ? `${row[column]}（受影响）` : row[column] ?? '—' }}
            </span>
            <template v-else>{{ row[column] ?? '—' }}</template>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无分群分析数据，可先登记人群分群</td>
        </tr>
      </tbody>
    </table>

    <section class="refresh-panel">
      <h3>刷新任务</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in runColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="run in refreshRuns" :key="String(run.id)">
            <td>{{ run['任务编号'] }}</td>
            <td>{{ run['分群编号'] }}</td>
            <td>
              <span class="tag" :class="runTagClass(String(run['状态'] ?? ''))">{{ run['状态'] }}</span>
            </td>
            <td>{{ run['进度'] }}</td>
            <td>{{ run['统计']?.['命中'] ?? 0 }}</td>
            <td>{{ run['统计']?.['重算'] ?? 0 }}</td>
            <td>{{ run['统计']?.['续做'] ?? 0 }}</td>
            <td>{{ run['统计']?.['冲突'] ?? 0 }}</td>
            <td>{{ run['批次号'] || '—' }}</td>
            <td class="run-note">{{ run['说明'] || '—' }}</td>
          </tr>
          <tr v-if="!refreshRuns.length">
            <td :colspan="runColumns.length" class="empty-state">暂无刷新任务，可执行周期刷新或单个分群刷新</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条分群分析记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

const ENDPOINT = '/api/segment'
const columns = ["分群编号", "分群名称", "筛选规则", "刷新周期", "最近计算时间", "最终规模", "上次成功规模", "status"]
const actions = ["提交规则校验", "生效分群", "失效分群", "周期刷新"]
const runColumns = ["任务编号", "分群编号", "状态", "进度", "命中", "重算", "续做", "冲突", "批次号", "说明"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const infoMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const refreshRuns = ref<Row[]>([])
const refreshStats = ref<Record<string, number>>({ '命中': 0, '重算': 0, '续做': 0, '冲突': 0 })

const stats = computed(() => [
  { label: '人群分群总数', value: total.value },
  { label: '已生效分群', value: rows.value.filter((row) => row.status === '已生效').length },
  { label: '受影响分群', value: rows.value.filter((row) => row.abnormal).length },
  { label: '快照命中', value: refreshStats.value['命中'] ?? 0 },
  { label: '重新计算', value: refreshStats.value['重算'] ?? 0 },
  { label: '断点续做', value: refreshStats.value['续做'] ?? 0 },
  { label: '冲突转复用', value: refreshStats.value['冲突'] ?? 0 },
])

function runTagClass(status: string) {
  if (status === '已完成') return 'ok'
  if (status === '进行中') return 'warn'
  return 'danger'
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '人群分群登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '分群分析动作未生效，请稍后重试')
    }
    infoMessage.value = payload.message || '操作完成'
    await Promise.all([reload(), loadRefresh()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分群分析操作失败'
    await loadRefresh()
  }
}

async function runBatch() {
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/refresh-batch`, { method: 'POST' })
    const payload = await response.json()
    if (!payload.ok) {
      errorMessage.value = payload.message || '周期刷新未全部完成'
    } else {
      infoMessage.value = payload.message || '周期刷新完成'
    }
    await Promise.all([reload(), loadRefresh()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '周期刷新执行失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('人群分群列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分群分析列表读取失败'
  }
}

async function loadRefresh() {
  try {
    const [statsResp, runsResp] = await Promise.all([
      request(`${ENDPOINT}/refresh-stats`),
      request(`${ENDPOINT}/refresh-runs?size=20`),
    ])
    if (statsResp.ok) {
      const payload = await statsResp.json()
      refreshStats.value = payload['统计'] ?? refreshStats.value
    }
    if (runsResp.ok) {
      const payload = await runsResp.json()
      refreshRuns.value = payload.items ?? []
    }
  } catch {
    // 刷新统计读取失败不打断页面主流程
  }
}

onMounted(() => {
  void reload()
  void loadRefresh()
})
</script>

<style scoped>
.page-actions { display: flex; gap: 8px; }
.info-text { color: #067647; font-size: 13px; margin: 0 0 10px; }
.refresh-panel { margin-top: 16px; }
.refresh-panel h3 { font-size: 14px; margin: 0 0 8px; }
.tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px; background: #eef2f6; }
.tag.ok { background: #dcfae6; color: #067647; }
.tag.warn { background: #fef0c7; color: #b54708; }
.tag.danger { background: #fee4e2; color: #b42318; }
.run-note { max-width: 360px; white-space: normal; color: var(--muted); }
</style>
