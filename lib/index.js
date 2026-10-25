/**
 * dsh-llm-sse-order — 上游 SSE 乱序容错（out-of-tree 插件，不改任何源码）。
 *
 * 背景：`@deepseek-ai/dsh-llm-deepseek` 的 `translate()` 对事件顺序极其严格：
 *
 *     if (!started) return malformed("event precedes message_start");
 *
 * 只要上游（api.deepseek.com/anthropic/v1/messages）偶发在 `message_start` 之前先推
 * 内容事件，或漏推 `content_block_start`，整轮就以 `MALFORMED_RESPONSE` 失败
 * （实测偶发，重试一次仍可能失败；0.2.1-rc.2 里这条 guard 依然在）。
 *
 * 这个插件在**传输层**做归一化：包装 `globalThis.fetch`，只处理
 * `text/event-stream` 的 `/messages` 响应，把流"修成"适配器期望的顺序：
 *
 *   1. 首个内容事件之前没有 `message_start` → 补一条合成的 `message_start`
 *      （它只承载 role/usage，补上不影响内容）；
 *   2. `content_block_delta` / `content_block_stop` 找不到对应块 → 按增量类型
 *      补一条合成的 `content_block_start`（text / thinking）；
 *   3. 缺 `content_block_start` 的 `input_json_delta`（拿不到 id/name，无法重建
 *      tool-call）→ 丢弃该帧并告警，避免把无效工具调用注入 Agent。
 *
 * 顺序正常时**逐帧原样透传**（不做任何改写），所以对现有链路零影响。
 * 每次修复都会打一条 `[dsh-llm-sse-order]` 警告，便于观察上游到底多久抽一次风。
 */

export const name = 'dsh-llm-sse-order'
/** 该插件不依赖任何 cordis 服务：只改全局 fetch。 */
export const inject = []

import { appendFileSync } from 'node:fs'

const MARK = Symbol.for('dsh.llmSseOrderFix.installed')

const TMP_DIR = process.env.TEMP || process.env.TMPDIR || '/tmp'
const LOAD_PATH = `${TMP_DIR}/dsh-llm-sse-order.load`
const DIAG_PATH = `${TMP_DIR}/dsh-llm-sse-order.log`

const CONTENT_EVENTS = new Set([
  'content_block_start',
  'content_block_delta',
  'content_block_stop',
  'message_delta',
  'message_stop',
])

const SYNTHETIC_MESSAGE_START =
  'event: message_start\n' +
  'data: {"type":"message_start","message":{"id":"dsh-synthetic-message","type":"message",' +
  '"role":"assistant","model":"unknown","content":[],"stop_reason":null,"stop_sequence":null,' +
  '"usage":{"input_tokens":0,"output_tokens":0}}}\n\n'

function syntheticBlockStart(index, kind) {
  const block =
    kind === 'thinking'
      ? '{"type":"thinking","thinking":"","signature":""}'
      : '{"type":"text","text":""}'
  return (
    'event: content_block_start\n' +
    `data: {"type":"content_block_start","index":${index},"content_block":${block}}\n\n`
  )
}

function shouldNormalize(url, response) {
  if (!response || !response.body) return false
  const target = String(url || '')
  // 只碰 Messages 端点（anthropic 风格）；其他端点不动
  if (!/\/messages(?:$|[?#])/.test(target)) return false
  if (response.status !== 200) return false
  // 不再要求 content-type === text/event-stream：实测失败的那几次响应
  // 根本没有 content-type，于是被这行挡掉、绕过了归一化。
  // 归一化器只认 `data:` 帧并按事件类型改写，非 SSE 正文会原样透传。
  return true
}

function blockKindForDelta(deltaType) {
  if (deltaType === 'thinking_delta' || deltaType === 'signature_delta') return 'thinking'
  return 'text'
}

/**
 * 把一条 SSE 流改写成"适配器能吃"的顺序。
 * @param {string} label - 用于告警的标识（主机名 + 路径）。
 * @returns {TransformStream<Uint8Array, Uint8Array>}
 */
function makeOrderFixer(label) {
  const decoder = new TextDecoder()
  const encoder = new TextEncoder()
  let buffer = ''
  let sawMessageStart = false
  const openBlocks = new Set()
  let repairedStart = 0
  let repairedBlocks = 0
  let droppedFrames = 0
  let frames = 0
  let firstType = '(none)'

  const diagnose = () => {
    try {
      appendFileSync(
        DIAG_PATH,
        `${new Date().toISOString()} stream=${label} frames=${frames} first=${firstType} ` +
          `fix_start=${repairedStart} fix_block=${repairedBlocks} drop_tool=${droppedFrames}\n`
      )
    } catch {}
  }

  const warnOnce = () => {
    if (repairedStart + repairedBlocks + droppedFrames === 0) return
    console.warn(
      `[dsh-llm-sse-order] ${label}: 修复 message_start=${repairedStart} ` +
        `block_start=${repairedBlocks} 丢弃工具增量=${droppedFrames}`
    )
  }

  // 修复发生后**立刻**落盘：流经常在 message_stop 之后被 cancel，
  // TransformStream 的 flush 不保证被调用，等到 close 再记日志会什么都看不到。
  let fixLogged = false
  const noteRepair = () => {
    if (fixLogged) return
    fixLogged = true
    diagnose()
    warnOnce()
  }

  const handleFrame = (frame, controller) => {
    const dataLines = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).trimStart())
    if (dataLines.length === 0) return frame

    let payload
    try {
      payload = JSON.parse(dataLines.join('\n'))
    } catch {
      return frame
    }
    if (!payload || typeof payload.type !== 'string') return frame
    frames += 1
    if (frames === 1) firstType = payload.type

    if (payload.type === 'message_start') {
      sawMessageStart = true
      return frame
    }
    if (!CONTENT_EVENTS.has(payload.type)) return frame

    let prefix = ''
    if (!sawMessageStart) {
      sawMessageStart = true
      repairedStart += 1
      prefix += SYNTHETIC_MESSAGE_START
      noteRepair()
    }

    const index = payload.index
    if (payload.type === 'content_block_start' && Number.isSafeInteger(index)) {
      openBlocks.add(index)
      return prefix + frame
    }

    if (payload.type === 'content_block_delta' || payload.type === 'content_block_stop') {
      if (!Number.isSafeInteger(index)) return prefix + frame
      if (!openBlocks.has(index)) {
        const deltaType = payload.type === 'content_block_delta' ? payload.delta?.type : undefined
        if (deltaType === 'input_json_delta') {
          // 缺 start 就不知道工具 id/name，凭空造一个会污染 Agent 的工具调用
          droppedFrames += 1
          noteRepair()
          return prefix
        }
        const kind = blockKindForDelta(deltaType)
        openBlocks.add(index)
        if (payload.type === 'content_block_delta') {
          repairedBlocks += 1
          prefix += syntheticBlockStart(index, kind)
          noteRepair()
        } else {
          // stop 之前补一个空块，再让它正常关闭
          repairedBlocks += 1
          prefix += syntheticBlockStart(index, 'text')
          noteRepair()
        }
      }
      return prefix + frame
    }
    return prefix + frame
  }

  const flush = (controller, final) => {
    let index = 0
    for (;;) {
      const match = /\r?\n\r?\n/.exec(buffer.slice(index))
      if (!match) break
      const end = index + match.index
      const frame = buffer.slice(index, end)
      const separator = match[0]
      const out = handleFrame(frame, controller)
      if (out) controller.enqueue(encoder.encode(out + separator))
      index = end + separator.length
    }
    buffer = buffer.slice(index)
    if (final && buffer) {
      const out = handleFrame(buffer, controller)
      if (out) controller.enqueue(encoder.encode(out))
      buffer = ''
    }
  }

  return new TransformStream({
    transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true })
      flush(controller, false)
    },
    flush(controller) {
      buffer += decoder.decode()
      flush(controller, true)
      warnOnce()
      diagnose()
    },
  })
}

export function apply() {
  const g = globalThis
  const marker = LOAD_PATH
  try {
    appendFileSync(marker, `${new Date().toISOString()} pid=${process.pid}\n`)
  } catch {}
  if (g[MARK]) return
  g[MARK] = true

  const original = g.fetch
  if (typeof original !== 'function') return

  g.fetch = function fetchWithSseOrderFix(input, init) {
    const url = typeof input === 'string' ? input : input?.url || ''
    return original.call(this, input, init).then((response) => {
      const type = response?.headers?.get?.('content-type') || ''
      // 请求级诊断：证明"这个进程的模型请求确实经过这里"，与流是否被 cancel 无关。
      // 同时记录全部响应头/请求体长度——没有它就无法解释"200 但没有 content-type"。
      if (String(url).includes('/messages')) {
        try {
          const headerKeys = response?.headers ? [...response.headers.keys()].join(',') : ''
          const bodyLen = typeof init?.body === 'string' ? init.body.length : -1
          appendFileSync(
            DIAG_PATH,
            `${new Date().toISOString()} req url=${url} status=${response?.status} ` +
              `method=${init?.method || ''} bodyLen=${bodyLen} ctype=${type || '-'} hdrs=${headerKeys}\n`
          )
        } catch {}
      }
      if (!shouldNormalize(url, response)) return response
      let label = url
      try {
        const parsed = new URL(url)
        label = `${parsed.host}${parsed.pathname}`
      } catch {}
      const headers = new Headers(response.headers)
      headers.delete('content-length')
      headers.delete('content-encoding')
      return new Response(response.body.pipeThrough(makeOrderFixer(label)), {
        status: response.status,
        statusText: response.statusText,
        headers,
      })
    })
  }
}
