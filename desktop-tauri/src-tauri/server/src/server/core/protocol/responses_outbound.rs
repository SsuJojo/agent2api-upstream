//! Responses 协议的**出站**转换：chat 请求 → Responses 上游请求、
//! 上游 Responses SSE → chat SSE（自定义提供商转发的 `responses` 分支）。
//!
//! ── 与 `responses.rs` 的方向关系 ─────────────────────────────
//! `responses.rs` 服务的是**下游入口**（`api::protocol` 的 `/v1/responses`）：
//! 把客户端发来的 Responses 请求翻成 chat、把回程的 chat 响应/SSE 翻回
//! Responses。本文件是它的**反方向**，服务自定义提供商的**上游**：
//! 转发层手里的统一货币是 chat，但这家上游说 Responses，于是
//!   - 请求：`responses_request_from_chat`（chat 体 → Responses 体，逐字段
//!     对着 `chat_from_responses` 反推）；
//!   - 响应：`ChatFromResponsesStream`（上游 Responses SSE → 标准 chat SSE，
//!     之后照走既有的 `ForwardStream` / 聚合器 —— 那两层只认 chat 帧）。
//!
//! ── 上游恒以 `stream:true` 被请求 ────────────────────────────
//! 与 chat 协议同一条策略（各家上游的流式才是完整能力；非流式下游由网关
//! 收流聚合）。因此响应侧只需要流式转换器；非流式 JSON 路径不会有上游
//! 来源。事件流的折法参考 OmniProxy 的 `responsesStreamToChat`，字段口径
//! 按本项目 chat 侧（`responses.rs` 回程消费的形状）收敛。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! 同 `mod.rs`：零 unwrap/expect/panic；解析失败退化「跳过该帧」。

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::{
    chat_frame, content_parts, content_text, is_truthy, json_number_of, json_text, native_tool,
    random_id, string_field, string_value, SseLineBuffer, FIELD_ENCRYPTED_CONTENT,
    FIELD_IS_ERROR, FIELD_REASONING_ITEM_ID,
};
use super::responses::{image_url_of, tool_output_parts, ConvertError, ToolOutput};
use super::tool_plan;

// ─── 请求：Chat → Responses ─────────────────────────────────

/// Chat Completions 请求体 → Responses 请求体。
///
/// ── 逐字段对着 [`super::responses::chat_from_responses`] 反推 ────
/// 那份转换把 Responses 请求折成 chat，字段映射全在里面；这里逐条走它的
/// 完整清单（漏一条的症状是「客户端参数静默失效」）：
///   - `model`（由调用方给上游真名，见 `providers::custom::forward`）
///   - `messages` → `instructions`（system 消息）+ `input`（message /
///     function_call / function_call_output 项）
///   - `stream` / `max_output_tokens`（← `max_completion_tokens` / `max_tokens`）
///   - `temperature` / `top_p` / `service_tier` / `parallel_tool_calls` /
///     `user` / `metadata`
///   - `tools`（chat 嵌套 function → Responses 扁平 function）
///   - `tool_choice`、`response_format` → `text.format`
///   - `reasoning_effort` → `reasoning:{effort, summary:"auto"}`
///   - `store: false`（本网关无状态；对上游同样不存）
///
/// 不支持的 chat 字段（`n` / `logprobs` 等）**原样丢弃**：Responses 没有
/// 对应形态，发送上游不认识的键只会换来一次 400。
pub fn responses_request_from_chat(chat: &Value, model: &str) -> Result<Value, ConvertError> {
    let Some(messages) = chat.get("messages").and_then(Value::as_array) else {
        return Err("缺少 messages 数组".to_string());
    };
    let mut instructions: Vec<String> = Vec::new();
    let mut items: Vec<Value> = Vec::new();
    for message in messages {
        let role = {
            let raw = string_field(message, "role").to_lowercase();
            if raw == "developer" {
                "system".to_string()
            } else if raw.is_empty() {
                "user".to_string()
            } else {
                raw
            }
        };
        match role.as_str() {
            // system / developer → 顶层 instructions（多条用空行拼接，
            // 与 anthropic 侧 system_text 的拼接口径一致）
            "system" => {
                let text = content_text(message.get("content").unwrap_or(&Value::Null));
                if !text.trim().is_empty() {
                    instructions.push(text);
                }
            }
            // tool 结果 → function_call_output 项（call_id 是配对的钥匙，
            // 缺了它上游在多轮里认不出这条结果属于哪次调用）
            "tool" => {
                // is_error 在 Responses 的 function_call_output 上没有独立
                // 字段（Chat/Anthropic 有）：折成输出文本前缀，让模型把失败
                // 当失败推理，而不是当正常输出继续跑。与 anthropic 出站的
                // is_error 恢复、参考实现的 "Error: " 前缀同一取向。
                let is_error = message.get(FIELD_IS_ERROR).and_then(Value::as_bool) == Some(true);
                let mut output = tool_output_to_responses(tool_output_parts(message.get("content")));
                if is_error {
                    output = mark_output_error(output);
                }
                items.push(json!({
                    "type": "function_call_output",
                    "call_id": string_field(message, "tool_call_id"),
                    "output": output,
                }));
            }
            "assistant" => push_assistant_items(&mut items, message),
            // user 与「认不出的角色」都当 user（与 chat_from_responses 的
            // 兜底同一取向：能跑比报错好）
            _ => {
                let content =
                    chat_content_to_responses(message.get("content").unwrap_or(&Value::Null), "user");
                items.push(json!({ "type": "message", "role": "user", "content": content }));
            }
        }
    }

    let mut out = Map::new();
    out.insert("model".to_string(), Value::String(model.to_string()));
    out.insert("input".to_string(), Value::Array(items));
    out.insert(
        "stream".to_string(),
        Value::Bool(chat.get("stream").and_then(Value::as_bool).unwrap_or(false)),
    );
    if !instructions.is_empty() {
        out.insert(
            "instructions".to_string(),
            Value::String(instructions.join("\n\n")),
        );
    }
    // max_output_tokens ← max_completion_tokens（Responses 口径的字段名），
    // 退到 max_tokens；非正数视为没给（0 / 负数发上去只会被上游拒）
    let max_output = ["max_completion_tokens", "max_tokens"]
        .iter()
        .find_map(|key| chat.get(*key).and_then(Value::as_i64).filter(|value| *value > 0));
    if let Some(value) = max_output {
        out.insert("max_output_tokens".to_string(), Value::from(value));
    }
    for key in ["temperature", "top_p", "service_tier", "parallel_tool_calls", "user", "metadata"] {
        if let Some(value) = chat.get(key).filter(|value| !value.is_null()) {
            out.insert(key.to_string(), value.clone());
        }
    }
    // 工具声明：函数工具翻译成 Responses 扁平形态；原生（服务端执行）声明只有
    // 「来源就是 Responses」的原样恢复（保真，含 web_search / tool_search 等
    // 宿主工具的一切字段），跨协议的不猜 —— 剔除并留痕（见 `native_tool` 模块头）
    let mut natives: Vec<Value> = Vec::new();
    if let Some(tools) = chat.get("tools").and_then(Value::as_array) {
        let converted: Vec<Value> = tools
            .iter()
            .filter_map(|tool| tool_to_responses(tool, &mut natives))
            .collect();
        if !converted.is_empty() {
            out.insert("tools".to_string(), Value::Array(converted));
        }
    }
    let mut choice_reason: Option<String> = None;
    if let Some(choice) = chat.get("tool_choice").filter(|value| is_truthy(value)) {
        // 点名的工具被剔除时 `tool_choice` 一并撤掉（理由同 `anthropic_outbound`）
        match native_tool::choice_conflict(choice, &natives) {
            Some(reason) => choice_reason = Some(reason),
            None => {
                out.insert("tool_choice".to_string(), tool_choice_to_responses(choice));
            }
        }
    }
    if !natives.is_empty() || choice_reason.is_some() {
        let dropped = native_tool::Downgrade { tools: natives, choice: choice_reason };
        crate::server::logging::log(
            "[Responses]",
            &dropped.describe(None, Some("目标上游按 responses 协议收，只认 responses 来源的原生声明")),
        );
    }
    // response_format → text.format（json_schema 的嵌套 → 扁平）
    if let Some(format) = chat.get("response_format").filter(|value| is_truthy(value)) {
        out.insert("text".to_string(), json!({ "format": format_to_responses(format) }));
    }
    // reasoning_effort → reasoning.effort（本网关各上游的「思考档位」统一
    // 从这里进；summary:"auto" 与参考实现同款 —— 让上游回思考摘要）
    if let Some(effort) = chat
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        out.insert(
            "reasoning".to_string(),
            json!({ "effort": effort, "summary": "auto" }),
        );
    }
    // store:false 与模块头同一理由（网关无状态，也不许上游存）
    out.insert("store".to_string(), Value::Bool(false));
    Ok(Value::Object(out))
}

/// 一条 assistant 消息 → 零到多个 input 项（顺序：reasoning → message →
/// function_call）。
///
/// 顺序不是随意的：回程的 `chat_from_responses` 按「reasoning 项在前、
/// 本轮 assistant 项在后」消费（`PendingReasoning`），反向也必须产出同一
/// 形状，多轮历史才能被上游正确读取。
fn push_assistant_items(items: &mut Vec<Value>, message: &Value) {
    let reasoning = {
        let from_field = string_field(message, "reasoning_content");
        if from_field.is_empty() {
            string_field(message, "reasoning")
        } else {
            from_field
        }
    };
    // 加密连续性载体（store=false 多轮）恢复进 reasoning 项：与 summary 文本
    // 可以并存（OpenAI 官方回传的形态就是两者同项），只有加密体时 summary
    // 给空数组。没有这个恢复，Codex / Grok CLI 的下一轮就接不上推理链 ——
    // 它们靠把上游回传的加密体原样带回下一轮请求来延续推理。
    let encrypted = message
        .get(FIELD_ENCRYPTED_CONTENT)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty());
    if !reasoning.is_empty() || encrypted.is_some() {
        // id 只在**带加密体、且确实拿到了上游真实 id**时给：
        //   - 没有加密载体的回放（下游 Messages 链路把 thinking 文本折回
        //     summary）本就不该带 id；
        //   - 有载体但没暂存到 id 时，伪造一个 `rs_` 配上真实加密体，严格上游
        //     （官方 Responses store:false）按 item id 查项会整轮拒绝 —— 比不带
        //     id 更糟。input 项的 id 本就可省。
        let mut item = json!({
            "type": "reasoning",
            "summary": if reasoning.is_empty() {
                Vec::<Value>::new()
            } else {
                vec![json!({ "type": "summary_text", "text": reasoning })]
            },
        });
        if let Some(encrypted) = encrypted {
            if let Some(object) = item.as_object_mut() {
                let id = message
                    .get(FIELD_REASONING_ITEM_ID)
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty());
                if let Some(id) = id {
                    object.insert("id".to_string(), Value::String(id.to_string()));
                }
                object.insert(
                    "encrypted_content".to_string(),
                    Value::String(encrypted.to_string()),
                );
            }
        }
        items.push(item);
    }
    // 正文：Responses 不需要空的 assistant message（只带工具调用的轮次
    // 只发 function_call 项）；空正文跳过也能避免上游把空消息判成错误
    let text = content_text(message.get("content").unwrap_or(&Value::Null));
    if !text.is_empty() {
        items.push(json!({
            "type": "message",
            "id": random_id("msg"),
            "role": "assistant",
            "content": [{ "type": "output_text", "text": text }],
        }));
    }
    if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in calls {
            let call_id = {
                let raw = string_field(call, "id");
                if raw.is_empty() {
                    random_id("call")
                } else {
                    raw
                }
            };
            let name = call
                .pointer("/function/name")
                .map(string_value)
                .unwrap_or_default();
            let arguments = {
                let raw = json_text(call.pointer("/function/arguments").unwrap_or(&Value::Null));
                if raw.is_empty() {
                    "{}".to_string()
                } else {
                    raw
                }
            };
            items.push(json!({
                "type": "function_call",
                "id": random_id("fc"),
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            }));
        }
    }
}

/// 拆开的工具输出 → Responses 的 `function_call_output.output`。
///
/// ── 与入站方向相反的取舍（不是笔误）──────────────────────────
/// 入站（`responses.rs`）图片**必须**挪出 tool 消息：Chat 不许 `tool` 角色带
/// 图片（OpenAI 直接 400，见 `responses::PendingImages`）。出站这边不用 ——
/// Responses 官方 schema 里 `output` 本来就是
/// `String | Array[ResponseInputText | ResponseInputImage | …]`，图表留在工具结果
/// 里才是保真形态（DeepSeek 的 Responses 文档同样声明 `input_image` 按真图片
/// 处理）。所以这边只是「别把它拍平成 JSON 文本」，不搬消息。
///
/// 没有图片时保持字符串形态：字符串对各家上游最友好，也是原来就在发的形状。
fn tool_output_to_responses(output: ToolOutput) -> Value {
    if output.images.is_empty() {
        return Value::String(output.text);
    }
    let mut parts: Vec<Value> = Vec::new();
    if !output.text.is_empty() {
        parts.push(json!({ "type": "input_text", "text": output.text }));
    }
    for image in &output.images {
        let url = image_url_of(image);
        if !url.is_empty() {
            parts.push(json!({ "type": "input_image", "image_url": url }));
        }
    }
    if parts.is_empty() {
        // 图片块全都取不到 url（只剩 file_id 之类）：退回文本，别发空数组上去
        return Value::String(output.text);
    }
    Value::Array(parts)
}

/// 给工具输出打上失败标记（`is_error` 的 Responses 表达）。
///
/// Responses 的 `function_call_output.output` 没有独立的 is_error 字段，
/// 与参考实现同款折成文本前缀：字符串直接加 `Error: `；带图的数组形态
/// 把前缀折进第一个 input_text，连文本块都没有时补一个，保证失败可见。
fn mark_output_error(output: Value) -> Value {
    match output {
        Value::Array(mut parts) => {
            let text_slot =
                parts.iter().position(|part| string_field(part, "type") == "input_text");
            match text_slot {
                Some(index) => {
                    let text = string_field(&parts[index], "text");
                    if let Some(object) = parts[index].as_object_mut() {
                        object.insert(
                            "text".to_string(),
                            Value::String(format!("Error: {text}")),
                        );
                    }
                }
                None => parts.insert(
                    0,
                    json!({ "type": "input_text", "text": "Error: (tool failed)" }),
                ),
            }
            Value::Array(parts)
        }
        Value::String(text) => {
            if text.is_empty() {
                Value::String("Error: (tool failed)".to_string())
            } else {
                Value::String(format!("Error: {text}"))
            }
        }
        other => other,
    }
}

/// Chat content（字符串或块数组）→ Responses 的 input content。
///
/// 口径对着 `chat_from_responses` 的 `content_to_chat` 反推：
///   - 纯文本（字符串 / 全文本块）保持该有的形态（user 可用字符串，
///     assistant 用 `[{type:"output_text"}]`）；
///   - 图片块 → `input_image`（url 取 `image_url` 的字符串或 `{url}` 形态）；
///   - 其余块（音频/文件等）**丢弃** —— 它们在 chat 侧本来就没有来源，
///     透传一个上游不认识的类型只会让整条请求 400。
fn chat_content_to_responses(content: &Value, role: &str) -> Value {
    let text_kind = if role == "assistant" { "output_text" } else { "input_text" };
    if let Some(text) = content.as_str() {
        if role == "assistant" {
            return if text.is_empty() {
                Value::String(String::new())
            } else {
                json!([{ "type": "output_text", "text": text }])
            };
        }
        return Value::String(text.to_string());
    }
    let parts = content_parts(content);
    if parts.is_empty() {
        return Value::String(String::new());
    }
    let mut out: Vec<Value> = Vec::new();
    for part in parts {
        if let Some(text) = part.as_str() {
            out.push(json!({ "type": text_kind, "text": text }));
            continue;
        }
        let kind = string_field(part, "type").to_lowercase();
        match kind.as_str() {
            "text" | "input_text" | "output_text" => {
                out.push(json!({ "type": text_kind, "text": string_field(part, "text") }));
            }
            "image_url" | "input_image" if role == "user" => {
                let url = image_url_of(part);
                if !url.is_empty() {
                    out.push(json!({ "type": "input_image", "image_url": url }));
                }
            }
            _ => {}
        }
    }
    if out.is_empty() {
        Value::String(String::new())
    } else {
        Value::Array(out)
    }
}

/// Chat 工具声明 → Responses 工具声明（扁平）。
///
/// `parameters` 缺省给空对象 schema（`nested_from_flat` 的同一兜底）；
/// `strict` 原样搬运（两协议同名）。
///
/// 原生（服务端执行）声明：来源是 Responses 的原样恢复（`natives` 不收，
/// 保真）；其余（Anthropic 来源、或 chat 入口的方言原生工具）收进 `natives`
/// 由调用方留痕剔除 —— 目标协议是 responses，别的原生类型没有对应形态。
fn tool_to_responses(tool: &Value, natives: &mut Vec<Value>) -> Option<Value> {
    if native_tool::is_native(tool) {
        if native_tool::origin_of(tool) == Some(native_tool::ORIGIN_RESPONSES) {
            return Some(native_tool::restore(tool));
        }
        natives.push(tool.clone());
        return None;
    }
    if !tool_plan::is_nested_function(tool) {
        // 非嵌套形态（裸 name/parameters）：出站没有可靠的翻译口径，
        // 丢弃而不是发一个半成品上去
        return None;
    }
    let function = tool.get("function")?;
    let name = string_field(function, "name");
    if name.is_empty() {
        return None;
    }
    let mut out = Map::new();
    out.insert("type".to_string(), Value::String("function".to_string()));
    out.insert("name".to_string(), Value::String(name));
    if let Some(description) = function.get("description").filter(|value| is_truthy(value)) {
        out.insert("description".to_string(), description.clone());
    }
    let parameters = function
        .get("parameters")
        .filter(|value| is_truthy(value))
        .cloned()
        .unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
    out.insert("parameters".to_string(), parameters);
    if let Some(strict) = function.get("strict") {
        out.insert("strict".to_string(), strict.clone());
    }
    Some(Value::Object(out))
}

/// Chat 的 tool_choice → Responses 的 tool_choice
/// （`{type:"function", function:{name}}` → `{type:"function", name}`；
/// 字符串形态两协议同名，原样保留）。
fn tool_choice_to_responses(choice: &Value) -> Value {
    if choice.is_string() {
        return choice.clone();
    }
    let name = {
        let nested = choice.pointer("/function/name").map(string_value).unwrap_or_default();
        if nested.is_empty() {
            string_field(choice, "name")
        } else {
            nested
        }
    };
    if name.is_empty() {
        return choice.clone();
    }
    json!({ "type": "function", "name": name })
}

/// Chat 的 `response_format` → Responses 的 `text.format`
/// （`{type:"json_schema", json_schema:{…}}` → `{type:"json_schema", …}`）。
fn format_to_responses(format: &Value) -> Value {
    let kind = string_field(format, "type");
    if kind != "json_schema" {
        return format.clone();
    }
    let mut out = Map::new();
    out.insert("type".to_string(), Value::String("json_schema".to_string()));
    for key in ["name", "description", "schema", "strict"] {
        if let Some(value) = format
            .pointer(&format!("/json_schema/{key}"))
            .filter(|value| !value.is_null())
        {
            out.insert(key.to_string(), value.clone());
        }
    }
    Value::Object(out)
}

// ─── 响应：Responses SSE → Chat SSE（流式）────────────────────

/// 上游 Responses SSE 字节流 → 标准 chat SSE 字节流（状态机）。
///
/// 输出帧的形状必须让下游既有消费层认得（`ForwardStream` 的 reasoning 合并
/// 与 usage 提取、聚合器的 `choices[0].delta` 读取、下游两种协议出口的
/// 反向翻译都吃这个形状），关键约定：
///   - 每个 chat chunk 都是 `data: {"id","object":"chat.completion.chunk",
///     "created","model","choices":[{"index":0,"delta":{…},"finish_reason":…}]}`
///   - `response.output_text.delta` → `delta.content`；
///     `response.reasoning_summary_text.delta` → `delta.reasoning_content`；
///     `response.function_call_arguments.delta` → `delta.tool_calls[…]`
///   - `response.completed` → 收尾帧（`finish_reason`）+ usage 帧
///     （`choices: []`，usage 折成 chat 口径）+ `data: [DONE]`
///   - `error` / `response.failed` → `data: {"error":{…}}` + `data: [DONE]`
///     （与 ForwardStream 的断流收尾同形状：聚合器据此转 502）
///
/// usage 帧会同时被两处读到（ForwardStream 的旁路提取 + 聚合器的 usage
/// 覆盖）—— 幂等覆盖，以最后一处为准，不影响含义。
pub struct ChatFromResponsesStream {
    buffer: SseLineBuffer,
    /// 输出帧的 model（发给上游的真名；ForwardStream 的回写层会按需改成
    /// 客户端请求名，此处不必区分）
    model: String,
    id: String,
    created: i64,
    started: bool,
    finished: bool,
    /// 已宣告的工具：主键 → 槽位（chat 的 tool_calls index +
    /// 是否已发过参数增量）
    tools: BTreeMap<String, ToolSlot>,
    /// 已转发的 reasoning 连续性字段，按 upstream item id 去重。
    reasoning_items: BTreeMap<String, ReasoningSlot>,
    /// 同一工具的**别名 → 主键**：官方 Responses 的 added 项同时带
    /// `call_id`（`call_…`）与 `id`（`fc_…`）两个身份，而增量 / 收尾事件
    /// 只带 `item_id`（值 = added 项的 `id`）。不归并别名的话，每个增量
    /// 事件都会被当成没见过的新工具，经 `ensure_tool` 补出一组随机 id、
    /// 空 name 的幽灵 tool_calls —— 下游因此看到翻倍的 tool_use。
    tool_aliases: BTreeMap<String, String>,
    next_tool_index: i64,
    /// 上游最近的 usage（Responses 口径；收尾时折成 chat 形态）
    usage: Option<Value>,
}

/// 一个工具的 chat 侧状态（key 是上游给的 item_id / call_id）
#[derive(Default)]
struct ToolSlot {
    index: i64,
    has_arguments: bool,
}

/// reasoning item 的 id 与 encrypted_content 可能分别出现在 added / done 事件。
#[derive(Default)]
struct ReasoningSlot {
    id_sent: bool,
    encrypted_sent: bool,
}

impl ChatFromResponsesStream {
    pub fn new(model: &str) -> Self {
        Self {
            buffer: SseLineBuffer::new(),
            model: model.to_string(),
            id: String::new(),
            created: crate::server::logging::now_ms() / 1000,
            started: false,
            finished: false,
            tools: BTreeMap::new(),
            reasoning_items: BTreeMap::new(),
            tool_aliases: BTreeMap::new(),
            next_tool_index: 0,
            usage: None,
        }
    }

    /// 吃一段上游字节，吐出要下发的 chat SSE 帧
    pub fn push(&mut self, chunk: &[u8]) -> Vec<bytes::Bytes> {
        let mut out = Vec::new();
        for payload in self.buffer.push(chunk) {
            match payload {
                None => out.extend(self.finish()),
                Some(data) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&data) {
                        out.extend(self.consume(&value));
                    }
                }
            }
        }
        out
    }

    /// 上游流结束（没有终态事件时的兜底收尾）
    pub fn finish(&mut self) -> Vec<bytes::Bytes> {
        if self.finished {
            return Vec::new();
        }
        // 先冲刷缓冲里残留的最后一帧
        let mut out = Vec::new();
        for payload in self.buffer.finish() {
            if let Some(data) = payload {
                if let Ok(value) = serde_json::from_str::<Value>(&data) {
                    out.extend(self.consume(&value));
                }
            }
        }
        out.extend(self.complete(None));
        out
    }

    /// 一个上游 Responses 事件 → 零到多个 chat 帧
    fn consume(&mut self, event: &Value) -> Vec<bytes::Bytes> {
        if self.finished {
            return Vec::new();
        }
        // usage 的位置各家不一：官方在 response.completed 的 response.usage
        // 里，部分网关直接放事件顶层。两处都收（事件流里的最近一次为准）
        let usage = event
            .get("usage")
            .filter(|usage| usage.is_object())
            .or_else(|| event.pointer("/response/usage").filter(|usage| usage.is_object()));
        if let Some(usage) = usage {
            self.usage = Some(usage.clone());
        }
        let kind = string_field(event, "type").to_lowercase();
        match kind.as_str() {
            "response.created" => {
                if let Some(id) = event
                    .pointer("/response/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                {
                    self.id = id.to_string();
                }
                self.start()
            }
            // 正文增量（refusal 是官方的拒答文本，chat 侧没有专用通道，
            // 与正文同路 —— 丢掉会让客户端看到一段空回答）
            "response.output_text.delta" | "response.refusal.delta" => {
                let Some(text) = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    return Vec::new();
                };
                let mut out = self.start();
                out.push(self.delta_frame(json!({ "content": text })));
                out
            }
            // 思考增量 → reasoning_content（chat 侧的统一表达）
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                let Some(text) = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                else {
                    return Vec::new();
                };
                let mut out = self.start();
                out.push(self.delta_frame(json!({ "reasoning_content": text })));
                out
            }
            // 工具宣告：拿到 id / name 才能在 chat 侧建立 tool_calls 槽位
            "response.output_item.added" => {
                let item = event.get("item").unwrap_or(&Value::Null);
                let item_kind = string_field(item, "type").to_lowercase();
                if item_kind == "reasoning" {
                    return self.reasoning_continuity(event, item);
                }
                if !matches!(item_kind.as_str(), "function_call" | "custom_tool_call") {
                    return Vec::new();
                }
                let mut out = self.start();
                self.ensure_tool(&mut out, event, item);
                out
            }
            "response.function_call_arguments.delta" => {
                let delta = string_field(event, "delta");
                if delta.is_empty() {
                    return Vec::new();
                }
                let mut out = self.start();
                let key = self.ensure_tool(&mut out, event, event);
                out.extend(self.push_arguments(&key, &delta));
                out
            }
            // 自由文本工具（custom）的输入增量：分片是裸文本，中途无法可靠
            // 剥出 JSON 壳（见 `ResponsesStream::tool_delta_event` 同款取舍），
            // 按原样进 chat 的 function.arguments；完整包壳只在一个分片都没
            // 来过的 `.done` 兜底里做（wrap_freeform_input）
            "response.custom_tool_call_input.delta" => {
                let delta = {
                    let raw = event.get("delta").unwrap_or(event.get("input").unwrap_or(&Value::Null));
                    json_text(raw)
                };
                if delta.is_empty() {
                    return Vec::new();
                }
                let mut out = self.start();
                let key = self.ensure_tool(&mut out, event, event);
                out.extend(self.push_arguments(&key, &delta));
                out
            }
            // 参数收尾：增量一个都没来过时（少数网关只发 .done）补完整参数
            "response.function_call_arguments.done" => {
                if self.tool_has_arguments(event, event) {
                    return Vec::new();
                }
                let arguments = {
                    let raw = json_text(event.get("arguments").unwrap_or(&Value::Null));
                    if raw.is_empty() { "{}".to_string() } else { raw }
                };
                let mut out = self.start();
                let key = self.ensure_tool(&mut out, event, event);
                out.extend(self.push_arguments(&key, &arguments));
                out
            }
            "response.custom_tool_call_input.done" => {
                if self.tool_has_arguments(event, event) {
                    return Vec::new();
                }
                let input = json_text(event.get("input").unwrap_or(&Value::Null));
                let arguments = super::freeform::wrap_freeform_input(&input);
                let mut out = self.start();
                let key = self.ensure_tool(&mut out, event, event);
                out.extend(self.push_arguments(&key, &arguments));
                out
            }
            // 项收尾：与上面 .done 同口径的补漏（两端都可能先到）
            "response.output_item.done" => {
                let item = event.get("item").unwrap_or(&Value::Null);
                let item_kind = string_field(item, "type").to_lowercase();
                if item_kind == "reasoning" {
                    return self.reasoning_continuity(event, item);
                }
                let arguments = match item_kind.as_str() {
                    "function_call" => {
                        let raw = json_text(item.get("arguments").unwrap_or(&Value::Null));
                        if raw.is_empty() { "{}".to_string() } else { raw }
                    }
                    "custom_tool_call" => {
                        super::freeform::wrap_freeform_input(&json_text(item.get("input").unwrap_or(&Value::Null)))
                    }
                    _ => return Vec::new(),
                };
                if self.tool_has_arguments(event, item) {
                    return Vec::new();
                }
                let mut out = self.start();
                let key = self.ensure_tool(&mut out, event, item);
                out.extend(self.push_arguments(&key, &arguments));
                out
            }
            // 上游错误（事件流里的失败）：折成 chat 错误帧 + [DONE]
            "error" | "response.failed" => self.fail(event),
            // 终态：收尾（usage 已在开头收过）。部分兼容网关只在最终 response
            // 里带完整 output，没有逐项 done；这里补扫 reasoning 连续性字段。
            "response.completed" | "response.done" => {
                let mut out = Vec::new();
                if let Some(items) = event.pointer("/response/output").and_then(Value::as_array) {
                    for item in items {
                        if string_field(item, "type").eq_ignore_ascii_case("reasoning") {
                            out.extend(self.reasoning_continuity(event, item));
                        }
                    }
                }
                out.extend(self.complete(None));
                out
            }
            "response.incomplete" => {
                // incomplete_details.reason 是 Responses 口径（官方值
                // "max_output_tokens" / "content_filter"），而 chat 的
                // finish_reason 只认 "length" / "content_filter" 等 —— 原样
                // 透传会在下游出口被折成 end_turn，把「输出被截断」谎报成
                // 「正常结束」。这里统一折算：截断 → length，拒答 → content_filter。
                let reason = event
                    .pointer("/response/incomplete_details/reason")
                    .map(string_value)
                    .unwrap_or_default();
                let finish = if reason == "content_filter" {
                    "content_filter"
                } else {
                    "length"
                };
                self.complete(Some(finish))
            }
            _ => Vec::new(),
        }
    }

    /// 把上游 reasoning 项的 id / encrypted_content 转成内部 Chat 帧。
    ///
    /// 两者可能分别出现在 `response.output_item.added` 与 `.done`，因此按
    /// item id 记录已转发状态；缺 id 时才退回 output_index 作为键。
    fn reasoning_continuity(&mut self, event: &Value, item: &Value) -> Vec<bytes::Bytes> {
        let id = string_field(item, "id");
        let encrypted = string_field(item, "encrypted_content");
        if id.is_empty() && encrypted.is_empty() {
            return Vec::new();
        }
        let key = if !id.is_empty() {
            id.clone()
        } else {
            format!(
                "output_index:{}",
                event.get("output_index").map(string_value).unwrap_or_default()
            )
        };
        let (send_id, send_encrypted) = {
            let slot = self.reasoning_items.entry(key).or_default();
            let send_id = !id.is_empty() && !slot.id_sent;
            let send_encrypted = !encrypted.is_empty() && !slot.encrypted_sent;
            if send_id {
                slot.id_sent = true;
            }
            if send_encrypted {
                slot.encrypted_sent = true;
            }
            (send_id, send_encrypted)
        };
        if !send_id && !send_encrypted {
            return Vec::new();
        }
        let mut delta = Map::new();
        if send_id {
            delta.insert(FIELD_REASONING_ITEM_ID.to_string(), Value::String(id));
        }
        if send_encrypted {
            delta.insert(
                FIELD_ENCRYPTED_CONTENT.to_string(),
                Value::String(encrypted),
            );
        }
        let mut out = self.start();
        out.push(self.delta_frame(Value::Object(delta)));
        out
    }

    /// 首帧（role assistant）：与官方 bridge 同款 —— 空响应或只有工具的流
    /// 也能建立 assistant 消息（下游按 delta 累积时不会拿到「无角色」的帧）
    fn start(&mut self) -> Vec<bytes::Bytes> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        if self.id.is_empty() {
            self.id = random_id("chatcmpl");
        }
        vec![self.delta_frame(json!({ "role": "assistant", "content": "" }))]
    }

    /// 解析或宣告一个工具，返回它的**主键**（chat 槽位键）。
    ///
    /// 已宣告过（含别名命中）：只把本次事件上新出现的身份补登记为别名，
    /// 不发帧 —— added / delta / done 三类事件的身份形态不一（官方 added
    /// 带 `call_id`+`id`，delta/done 只带 `item_id`），靠别名归并落同一个槽。
    /// 未宣告过：以第一个可用身份为主键建槽、其余身份记为别名，并补发
    /// chat 的 tool_calls 首帧（id / name / 空参数）进 `out`。
    /// 无任何可用身份的事件返回空串（调用侧的 push_arguments 会静默跳过）。
    fn ensure_tool(&mut self, out: &mut Vec<bytes::Bytes>, event: &Value, item: &Value) -> String {
        if let Some(primary) = self.resolve_tool_key(event, item) {
            for key in tool_identity_keys(event, item) {
                if key != primary && !self.tools.contains_key(&key) {
                    // 已登记到别的主键的别名不覆盖：宁可这个事件落到旧槽，也不能
                    // 把两条工具的身份链悄悄接在一起。
                    self.tool_aliases.entry(key).or_insert_with(|| primary.clone());
                }
            }
            return primary;
        }
        let keys = tool_identity_keys(event, item);
        let Some(primary) = keys.first().cloned() else {
            return String::new();
        };
        let index = self.next_tool_index;
        self.next_tool_index += 1;
        self.tools.insert(primary.clone(), ToolSlot { index, has_arguments: false });
        for key in keys.iter().skip(1) {
            self.tool_aliases
                .entry(key.clone())
                .or_insert_with(|| primary.clone());
        }
        let call_id = {
            let raw = string_field(item, "call_id");
            if raw.is_empty() {
                let id = string_field(item, "id");
                if id.is_empty() { random_id("call") } else { id }
            } else {
                raw
            }
        };
        let name = string_field(item, "name");
        out.push(self.delta_frame(json!({
            "tool_calls": [{
                "index": index,
                "id": call_id,
                "type": "function",
                "function": { "name": name, "arguments": "" },
            }],
        })));
        primary
    }

    /// 把一个事件 / 项上的身份解析到已登记槽位的主键（含别名回查）
    fn resolve_tool_key(&self, event: &Value, item: &Value) -> Option<String> {
        for key in tool_identity_keys(event, item) {
            if self.tools.contains_key(&key) {
                return Some(key);
            }
            if let Some(primary) = self.tool_aliases.get(&key) {
                return Some(primary.clone());
            }
        }
        None
    }

    /// 该工具（按全部身份键解析）是否已有参数增量落地
    fn tool_has_arguments(&self, event: &Value, item: &Value) -> bool {
        self.resolve_tool_key(event, item)
            .map(|key| self.has_arguments(&key))
            .unwrap_or(false)
    }

    /// 追加一段工具参数（槽位不存在时静默跳过 —— 没宣告过就没有 index 可挂）
    fn push_arguments(&mut self, key: &str, delta: &str) -> Vec<bytes::Bytes> {
        let Some(slot) = self.tools.get_mut(key) else {
            return Vec::new();
        };
        slot.has_arguments = true;
        let index = slot.index;
        vec![self.delta_frame(json!({
            "tool_calls": [{ "index": index, "function": { "arguments": delta } }],
        }))]
    }

    fn has_arguments(&self, key: &str) -> bool {
        self.tools.get(key).map(|slot| slot.has_arguments).unwrap_or(false)
    }

    /// 收尾：finish_reason 帧 + usage 帧 + [DONE]（只做一次）
    fn complete(&mut self, incomplete_reason: Option<&str>) -> Vec<bytes::Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut out = self.start();
        let finish_reason = match incomplete_reason {
            Some(reason) => reason.to_string(),
            // Responses 没有 chat 的 finish_reason，按输出形态推导：
            // 有工具调用 → tool_calls；否则 stop（length/content_filter
            // 由 response.incomplete 的 reason 直接给出）
            None => {
                if self.tools.is_empty() {
                    "stop".to_string()
                } else {
                    "tool_calls".to_string()
                }
            }
        };
        out.push(self.finish_frame(&finish_reason));
        if let Some(usage) = self.usage.take() {
            // usage 帧（choices 空数组）：ForwardStream 的旁路提取与聚合器
            // 都从这一帧读（形状对齐官方 include_usage 的收尾帧）
            out.push(chat_frame(&json!({
                "id": self.id,
                "object": "chat.completion.chunk",
                "created": self.created,
                "model": self.model,
                "choices": [],
                "usage": chat_usage_from_responses(&usage),
            })));
        }
        out.push(bytes::Bytes::from_static(b"data: [DONE]\n\n"));
        out
    }

    /// 上游错误 → chat 错误帧 + [DONE]（只做一次）
    fn fail(&mut self, event: &Value) -> Vec<bytes::Bytes> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let error = event
            .get("error")
            .filter(|error| is_truthy(error))
            .or_else(|| event.pointer("/response/error").filter(|error| is_truthy(error)));
        let message = match error {
            Some(error) => {
                let text = string_field(error, "message");
                if text.is_empty() { string_value(error) } else { text }
            }
            None => string_value(event),
        };
        let message = if message.trim().is_empty() {
            "上游流式返回错误".to_string()
        } else {
            message
        };
        vec![
            chat_frame(&json!({
                "error": { "message": message, "type": "upstream_error" },
            })),
            bytes::Bytes::from_static(b"data: [DONE]\n\n"),
        ]
    }

    /// 一个 delta 帧（`choices[0].delta = delta`，finish_reason 为 null）
    fn delta_frame(&self, delta: Value) -> bytes::Bytes {
        chat_frame(&json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{ "index": 0, "delta": delta, "finish_reason": Value::Null }],
        }))
    }

    /// 收尾帧（空 delta + finish_reason）
    fn finish_frame(&self, finish_reason: &str) -> bytes::Bytes {
        chat_frame(&json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{ "index": 0, "delta": {}, "finish_reason": finish_reason }],
        }))
    }
}

/// 一个事件 / 项上可用的工具身份，按优先序（call_id / id / item_id /
/// output_index）收齐并去重。
///
/// 同一次调用的 added / delta / done 事件身份形态**不一**：官方 Responses 的
/// added 项同时带 `call_id`（`call_…`）与 `id`（`fc_…`），而增量 / 收尾事件
/// 只带 `item_id`（值 = added 项的 `id`）。槽位解析必须对着这份**全集**做
/// 别名归并（见 `ChatFromResponsesStream::tool_aliases`），不能指望单一字段
/// 在所有事件里都一致 —— 否则官方形态上游的每次增量都会新建幽灵槽。
fn tool_identity_keys(event: &Value, item: &Value) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for text in [
        string_field(item, "call_id"),
        string_field(item, "id"),
        string_field(event, "item_id"),
    ] {
        if !text.is_empty() && !keys.contains(&text) {
            keys.push(text);
        }
    }
    // `output_index` 是**位置**不是身份：只在事件一个强身份都没带时才兜底，且用
    // `idx:` 前缀与真 id 分属不同命名空间。把它无条件并进同一组键会让「同一
    // output_index 上的另一个工具」被并进已有槽位（兼容网关常见：每次宣告都
    // 从 0 开始编号，但 call_id / id 是新的）。
    if keys.is_empty() {
        let output_index = event.get("output_index").map(string_value).unwrap_or_default();
        if !output_index.is_empty() {
            keys.push(format!("idx:{output_index}"));
        }
    }
    keys
}

/// Responses 的 usage → chat 的 usage（字段名与明细结构按 chat 口径折）。
///
/// 与 `responses.rs::usage_to_responses` 互为反函数，字段清单逐条对照：
/// `input_tokens`→`prompt_tokens`、`output_tokens`→`completion_tokens`、
/// `input_tokens_details.cached_tokens`→`prompt_tokens_details.cached_tokens`、
/// `output_tokens_details.reasoning_tokens`→`completion_tokens_details.reasoning_tokens`。
fn chat_usage_from_responses(usage: &Value) -> Value {
    let input = json_number_of(usage, &["input_tokens", "prompt_tokens"]);
    let output = json_number_of(usage, &["output_tokens", "completion_tokens"]);
    let cached = usage
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_i64)
        .filter(|value| *value != 0)
        .unwrap_or_else(|| json_number_of(usage, &["cache_read_input_tokens"]));
    let reasoning = usage
        .pointer("/output_tokens_details/reasoning_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut out = Map::new();
    out.insert("prompt_tokens".to_string(), Value::from(input));
    if cached != 0 {
        out.insert(
            "prompt_tokens_details".to_string(),
            json!({ "cached_tokens": cached }),
        );
    }
    out.insert("completion_tokens".to_string(), Value::from(output));
    if reasoning != 0 {
        out.insert(
            "completion_tokens_details".to_string(),
            json!({ "reasoning_tokens": reasoning }),
        );
    }
    out.insert(
        "total_tokens".to_string(),
        Value::from(json_number_of(usage, &["total_tokens"]).max(input + output)),
    );
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 喂给状态机一个上游 Responses SSE 事件
    fn feed(stream: &mut ChatFromResponsesStream, event: &Value) -> Vec<bytes::Bytes> {
        let line = format!("data: {}\n\n", serde_json::to_string(event).unwrap());
        stream.push(line.as_bytes())
    }

    fn text_of(frames: &[bytes::Bytes]) -> String {
        frames.iter().map(|frame| String::from_utf8_lossy(frame).to_string()).collect()
    }

    /// 收集全部 tool_calls **首宣告**帧的 id（delta.tool_calls 里非空 id 的项）
    fn announced_tool_ids(frames: &[bytes::Bytes]) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();
        for frame in frames {
            let text = String::from_utf8_lossy(frame);
            let Some(data) = text.strip_prefix("data: ") else { continue };
            let Ok(value) = serde_json::from_str::<Value>(data.trim()) else { continue };
            for call in value
                .pointer("/choices/0/delta/tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let id = string_field(call, "id");
                if !id.is_empty() {
                    ids.push(id);
                }
            }
        }
        ids
    }

    /// 收集全部参数增量所挂的 tool_calls index
    fn argument_delta_indexes(frames: &[bytes::Bytes]) -> Vec<i64> {
        let mut indexes: Vec<i64> = Vec::new();
        for frame in frames {
            let text = String::from_utf8_lossy(frame);
            let Some(data) = text.strip_prefix("data: ") else { continue };
            let Ok(value) = serde_json::from_str::<Value>(data.trim()) else { continue };
            for call in value
                .pointer("/choices/0/delta/tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let arguments =
                    call.pointer("/function/arguments").and_then(Value::as_str).unwrap_or("");
                if !arguments.is_empty() {
                    indexes.push(call.get("index").and_then(Value::as_i64).unwrap_or(-1));
                }
            }
        }
        indexes
    }

    /// 回归：官方 Responses 形态下 added 带 call_id+id 双身份、增量只带
    /// item_id —— 三类事件必须落到同一个槽，不许出现随机 id 的幽灵槽。
    #[test]
    fn official_shape_deltas_reuse_the_added_tool_slot() {
        let mut stream = ChatFromResponsesStream::new("gpt-5.2");
        let mut all: Vec<bytes::Bytes> = Vec::new();
        all.extend(feed(&mut stream, &json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": { "type": "function_call", "id": "fc_1", "call_id": "call_1",
                      "name": "get_weather", "arguments": "" },
        })));
        all.extend(feed(&mut stream, &json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_1",
            "output_index": 0,
            "delta": "{\"city\":",
        })));
        all.extend(feed(&mut stream, &json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_1",
            "output_index": 0,
            "delta": "\"Paris\"}",
        })));
        assert_eq!(announced_tool_ids(&all), vec!["call_1".to_string()]);
        assert_eq!(argument_delta_indexes(&all), vec![0, 0]);
        all.extend(feed(&mut stream, &json!({
            "type": "response.completed",
            "response": { "id": "resp_1", "usage": { "input_tokens": 10, "output_tokens": 5 } },
        })));
        let text = text_of(&all);
        assert!(text.contains("\"finish_reason\":\"tool_calls\""), "{text}");
    }

    /// 少数网关只发 .done 不发增量：全量参数要经别名解析补进 added 的原槽
    #[test]
    fn done_backfills_arguments_into_the_same_slot() {
        let mut stream = ChatFromResponsesStream::new("gpt-5.2");
        let mut all: Vec<bytes::Bytes> = Vec::new();
        all.extend(feed(&mut stream, &json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": { "type": "function_call", "id": "fc_9", "call_id": "call_9",
                      "name": "run", "arguments": "" },
        })));
        all.extend(feed(&mut stream, &json!({
            "type": "response.function_call_arguments.done",
            "item_id": "fc_9",
            "arguments": "{\"ok\":true}",
        })));
        assert_eq!(announced_tool_ids(&all), vec!["call_9".to_string()]);
        assert_eq!(argument_delta_indexes(&all), vec![0]);
    }

    /// response.incomplete 的官方 reason "max_output_tokens" 不是合法 chat
    /// finish_reason —— 原样透传会在下游出口被折成 end_turn，把截断谎报成
    /// 正常结束。必须折成 "length"。
    #[test]
    fn incomplete_max_output_tokens_maps_to_length() {
        let mut stream = ChatFromResponsesStream::new("gpt-5.2");
        feed(&mut stream, &json!({
            "type": "response.created",
            "response": { "id": "resp_2" },
        }));
        let all = feed(&mut stream, &json!({
            "type": "response.incomplete",
            "response": { "id": "resp_2", "incomplete_details": { "reason": "max_output_tokens" } },
        }));
        let text = text_of(&all);
        assert!(text.contains("\"finish_reason\":\"length\""), "{text}");
        assert!(text.contains("data: [DONE]"), "{text}");
    }

    #[test]
    fn incomplete_content_filter_maps_to_content_filter() {
        let mut stream = ChatFromResponsesStream::new("gpt-5.2");
        feed(&mut stream, &json!({
            "type": "response.created",
            "response": { "id": "resp_3" },
        }));
        let all = feed(&mut stream, &json!({
            "type": "response.incomplete",
            "response": { "id": "resp_3", "incomplete_details": { "reason": "content_filter" } },
        }));
        assert!(text_of(&all).contains("\"finish_reason\":\"content_filter\""));
    }

    /// tool_result.is_error 在 Responses 上没有独立字段：折成 "Error: " 前缀，
    /// 让模型把失败当失败推理（此前被静默丢弃）。
    #[test]
    fn tool_error_output_is_marked_for_responses_upstream() {
        let chat = json!({
            "model": "gpt-5.2",
            "messages": [
                { "role": "user", "content": "hi" },
                { "role": "assistant", "content": null, "tool_calls": [
                    { "id": "call_1", "type": "function",
                      "function": { "name": "f", "arguments": "{}" } },
                ] },
                { "role": "tool", "tool_call_id": "call_1", "content": "boom",
                  "_wb_is_error": true },
            ],
        });
        let body = responses_request_from_chat(&chat, "gpt-5.2").unwrap();
        assert_eq!(body["input"][2]["type"], "function_call_output");
        assert_eq!(body["input"][2]["output"], "Error: boom");
    }

    /// 带图的失败结果：前缀折进数组里的第一个 input_text，图片块保留
    #[test]
    fn tool_error_marks_the_text_part_inside_array_output() {
        let chat = json!({
            "model": "m",
            "messages": [
                { "role": "tool", "tool_call_id": "call_2", "_wb_is_error": true,
                  "content": [
                      { "type": "text", "text": "boom" },
                      { "type": "image_url", "image_url": { "url": "data:image/png;base64,AAAA" } },
                  ] },
            ],
        });
        let body = responses_request_from_chat(&chat, "m").unwrap();
        let output = body["input"][0]["output"].as_array().unwrap();
        assert_eq!(output[0]["type"], "input_text");
        assert_eq!(output[0]["text"], "Error: boom");
        assert_eq!(output[1]["type"], "input_image");
    }

    /// 回放的 reasoning 项在**没有加密载体**时不得伪造 rs_ id —— 严格校验
    /// item id 的上游（官方 Responses store:false）会按「查无此项」拒绝整轮。
    #[test]
    fn replayed_reasoning_without_encrypted_carries_no_fabricated_id() {
        let chat = json!({
            "model": "m",
            "messages": [
                { "role": "assistant", "content": "answer", "reasoning_content": "thought" },
            ],
        });
        let body = responses_request_from_chat(&chat, "m").unwrap();
        let item = &body["input"][0];
        assert_eq!(item["type"], "reasoning");
        assert!(item.get("id").is_none(), "无加密载体时不应伪造 rs_ id: {item}");
        assert_eq!(item["summary"][0]["text"], "thought");
    }

    /// 带加密载体（Responses 下游链路）时保留 id + encrypted_content 的
    /// 官方回传形态不变。
    #[test]
    fn reasoning_with_encrypted_content_keeps_an_id() {
        let chat = json!({
            "model": "m",
            "messages": [
                { "role": "assistant", "content": "answer",
                  "reasoning_content": "thought", "_wb_encrypted_content": "enc" },
            ],
        });
        let body = responses_request_from_chat(&chat, "m").unwrap();
        let item = &body["input"][0];
        assert_eq!(item["type"], "reasoning");
        assert!(item.get("id").is_some(), "带加密体时应保留 id 形态: {item}");
        assert_eq!(item["encrypted_content"], "enc");
    }
}
