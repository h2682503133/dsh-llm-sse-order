/**
 * 本地自测：不起 DSH，直接验证 fetch 包装后的流式归一化。
 *   node test/order-fix.test.mjs
 */
import assert from 'node:assert/strict'

const URL_MESSAGES = 'https://api.deepseek.com/anthropic/v1/messages'

function sse(type, payload) {
  return `event: ${type}\ndata: ${JSON.stringify(payload)}\n\n`
}

function responseFrom(text, headers = { 'content-type': 'text/event-stream' }, chunkSize = 7) {
  const bytes = new TextEncoder().encode(text)
  const stream = new ReadableStream({
    start(controller) {
      for (let i = 0; i < bytes.length; i += chunkSize) {
        controller.enqueue(bytes.slice(i, i + chunkSize))
      }
      controller.close()
    },
  })
  return new Response(stream, { status: 200, headers })
}

const originalFetch = globalThis.fetch
globalThis.__fixture = ''
globalThis.__headers = { 'content-type': 'text/event-stream' }
globalThis.fetch = async (input) => {
  const url = typeof input === 'string' ? input : input.url
  if (!url.includes('/messages')) return originalFetch(input)
  return responseFrom(globalThis.__fixture, globalThis.__headers)
}

const plugin = await import('../lib/index.js')
plugin.apply({})

async function collect() {
  const res = await fetch(URL_MESSAGES, { method: 'POST' })
  return await res.text()
}

function frames(text) {
  return text
    .split(/\r?\n\r?\n/)
    .filter((chunk) => chunk.trim())
    .map((chunk) => {
      const line = chunk.split(/\r?\n/).find((l) => l.startsWith('data:'))
      return JSON.parse(line.slice(5).trim())
    })
}

// A. 缺 message_start
globalThis.__fixture =
  sse('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hi' } }) +
  sse('content_block_stop', { type: 'content_block_stop', index: 0 })
{
  const out = frames(await collect())
  assert.equal(out[0].type, 'message_start', 'A: 应补 message_start')
  assert.deepEqual(out.map((e) => e.type), [
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
  ])
  assert.equal(out[2].delta.text, 'hi', 'A: 正文不能丢')
}

// B. 缺 content_block_start
globalThis.__fixture =
  sse('message_start', { type: 'message_start', message: { usage: { input_tokens: 3, output_tokens: 0 } } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'yo' } }) +
  sse('content_block_stop', { type: 'content_block_stop', index: 0 })
{
  const out = frames(await collect())
  assert.deepEqual(out.map((e) => e.type), [
    'message_start',
    'content_block_start',
    'content_block_delta',
    'content_block_stop',
  ])
  assert.equal(out[1].content_block.type, 'text')
  assert.equal(out[2].index, 0, 'B: 合成块的 index 必须对上')
}

// C. thinking_delta 缺块 → 合成 thinking 块
globalThis.__fixture =
  sse('message_start', { type: 'message_start', message: { usage: { input_tokens: 1, output_tokens: 0 } } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'thinking_delta', thinking: '想' } })
{
  const out = frames(await collect())
  assert.equal(out[1].type, 'content_block_start')
  assert.equal(out[1].content_block.type, 'thinking')
}

// D. 缺 start 的 tool 增量 → 丢弃，不造无效工具调用
globalThis.__fixture =
  sse('message_start', { type: 'message_start', message: { usage: { input_tokens: 1, output_tokens: 0 } } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: '{"a":1}' } })
{
  const out = frames(await collect())
  assert.deepEqual(out.map((e) => e.type), ['message_start'])
}

// E. 顺序正常 → 逐帧原样透传
globalThis.__fixture =
  sse('message_start', { type: 'message_start', message: { usage: { input_tokens: 2, output_tokens: 0 } } }) +
  sse('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'ok' } }) +
  sse('content_block_stop', { type: 'content_block_stop', index: 0 }) +
  sse('message_stop', { type: 'message_stop' })
{
  const raw = await collect()
  assert.equal(raw, globalThis.__fixture, 'E: 正常流必须字节级透传')
}

// F. 非 SSE 正文（JSON 错误体）在不带 content-type 时也要原样透传
globalThis.__headers = {}
globalThis.__fixture = '{"error":{"message":"rate limited"}}'
{
  const raw = await collect()
  assert.equal(raw, globalThis.__fixture, 'F: 非 SSE 正文不能被改写')
}

// G. 200 但**没有 content-type**（实测失败响应就长这样）→ 仍要归一化
globalThis.__headers = {}
globalThis.__fixture =
  sse('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } }) +
  sse('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'x' } }) +
  sse('content_block_stop', { type: 'content_block_stop', index: 0 })
{
  const out = frames(await collect())
  assert.equal(out[0].type, 'message_start', 'G: 无 content-type 也要补 message_start')
}

console.log('ALL OK — 7 cases')
