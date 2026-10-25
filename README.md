# dsh-llm-sse-order

DSH 插件：给 DeepSeek Messages 的 SSE 流做**顺序容错**，不改任何 DSH 源码。

## 为什么需要

`@deepseek-ai/dsh-llm-deepseek` 的 `translate()` 要求事件严格按
`message_start → content_block_* → message_delta/message_stop` 出现，否则：

```text
DeepSeek Messages stream: event precedes message_start
```

整轮以 `MALFORMED_RESPONSE` 失败（偶发，重试一次也可能继续失败）。
该 guard 在 0.2.1-rc.2 里仍然存在，升级不解决。

## 做什么

包装 `globalThis.fetch`，**只处理** `content-type: text/event-stream` 且路径匹配
`/messages` 的响应：

1. 首个内容事件之前缺 `message_start` → 补一条合成 `message_start`（只承载 role/usage）；
2. `content_block_delta` / `content_block_stop` 找不到块 → 按增量类型补 `content_block_start`
   （`thinking_delta`/`signature_delta` → thinking；其余 → text）；
3. 缺 `content_block_start` 的 `input_json_delta` → 丢弃该帧并告警（拿不到 id/name，
   凭空造会污染 Agent 的工具调用）。

顺序正常时**逐帧原样透传**；每次修复打一条 `[dsh-llm-sse-order]` 警告。

## 安装

```powershell
dsh plugin --profile web add link:D:/deepseek_workspace/dsh-llm-sse-order
# 然后把 "dsh-llm-sse-order" 加进 ~/.dsh/profiles/web/package.json 的 dsh.profile.bundles
```

重启 `dsh web` 后生效（`fetch` 在启动时被包装）。
