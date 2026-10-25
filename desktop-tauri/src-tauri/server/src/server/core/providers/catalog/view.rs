//! 模型的管理视图和对外视图。所有对外出口使用相同的开启绑定集合。

use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::capability;
use crate::server::core::key_scope::{self, KeyScope};
use crate::server::core::model_meta;
use crate::server::core::model_rules;
use crate::server::core::models::{list_response_from, model_id, suggest_from};
use crate::server::core::providers::{kind_from_id, kind_id, ProviderKind};

use super::routing::{entry_id_in_manifest, public_items_for_provider};
use super::{active_manifests, aggregate_source, manifest_for, refresh_meta};

/// 先在各提供商内判断绑定，再按对外名去重；不能先合并上游 ID 再拼别名，
/// 否则某家关闭的映射会借另一家的同名上游重新出现在列表中。
fn public_builtin_items(active: &[(ProviderKind, Vec<Value>)]) -> Vec<(String, Value)> {
    let rules = model_rules::current();
    let mut claimed = HashSet::new();
    let mut result = Vec::new();
    for (kind, manifest) in active {
        let provider = kind_id(*kind);
        result.extend(public_items_for_provider(
            &rules,
            provider,
            manifest,
            &mut claimed,
        ));
    }
    result
}

pub fn models_response(store: &AccountStore, scope: Option<&KeyScope>) -> Value {
    let active: Vec<_> = active_manifests(store)
        .into_iter()
        .filter(|(kind, _)| key_scope::allows_provider(scope, kind_id(*kind)))
        .collect();
    let (source, refreshed_at) = aggregate_source(&active);
    let mut data: Vec<_> = public_builtin_items(&active)
        .into_iter()
        .map(|(_, item)| item)
        .filter(|item| key_scope::allows_model(scope, &model_id(item)))
        .collect();
    let builtin_count = data.len();
    super::super::custom::append_models_response(store, scope, &mut data);
    // ── 插件家（运行时装进来的那些）────────────────────────────
    // `active_manifests` 的键是 `ProviderKind`（编译期枚举），装不进运行期
    // 才出现的 id。少了这一段，装了插件、模型也刷到了，`/v1/models` 里却
    // 看不到它的任何模型 —— 客户端点名插件模型时还会被入口校验判成
    // 「模型不存在」（见 `pipeline::resolve_model`）。追加在末尾，内置家的
    // 条目顺序因此逐字不变。
    append_plugin_models(store, scope, &mut data);
    // 自定义家 / 插件家贡献了条目时，这份列表就不再是单/多家内置的来源了 ——
    // 与「多家内置」同一个词（aggregate），下游据此知道列表是拼出来的。
    let source = if data.len() > builtin_count {
        "aggregate"
    } else {
        source
    };
    list_response_from(data, source, refreshed_at)
}

/// 把已装入插件的模型并进对外清单（返回追加的条数）。
///
/// ── 判据与内置家同源 ────────────────────────────────────────
/// 内置家要「有可用账号」才广告（`provider_available`），插件家同样：
/// 没有账号时广告出去只会让客户端选到一条转发必然 503 的模型。
fn append_plugin_models(
    store: &AccountStore,
    scope: Option<&KeyScope>,
    data: &mut Vec<Value>,
) -> usize {
    let mut claimed: HashSet<String> = data
        .iter()
        .map(|item| model_id(item).to_lowercase())
        .collect();
    let before = data.len();
    let rules = model_rules::current();
    for id in crate::server::core::plugin_host::installed_ids() {
        if !key_scope::allows_provider(scope, &id) || store.accounts_for_provider(&id).is_empty() {
            continue;
        }
        let Some(adapter) = crate::server::core::plugin_host::lookup(&id) else {
            continue;
        };
        let manifest = adapter.list_models();
        data.extend(
            public_items_for_provider(&rules, &id, &manifest, &mut claimed)
                .into_iter()
                .map(|(_, item)| item)
                .filter(|item| key_scope::allows_model(scope, &model_id(item))),
        );
    }
    data.len() - before
}

/// 一家可用提供商都没有时（没加账号），入口校验要跳过「广告里有才放行」与
/// Key 白名单两条判定，把「没有可用账号」那句更可操作的错误留给转发层 ——
/// 与改造前的两条例外逐字同源（见 `pipeline::resolve_model` 的说明）。
///
/// 插件家同样算「可用」：只有插件家账号时若这里报 false，入口校验会整段跳过
/// —— 那意味着任何模型名都被放行到转发层，与「装了插件却像是没装」是同一个
/// 症状的另一面。
pub fn has_available_providers(store: &AccountStore) -> bool {
    if !active_manifests(store).is_empty() {
        return true;
    }
    crate::server::core::plugin_host::installed_ids()
        .into_iter()
        .any(|id| !store.accounts_for_provider(&id).is_empty())
}

pub fn advertised_model_ids(store: &AccountStore) -> Vec<String> {
    models_response(store, None)
        .get("data")
        .and_then(Value::as_array)
        .map(|items| items.iter().map(model_id).collect())
        .unwrap_or_default()
}

pub fn advertised_manifest_contains(store: &AccountStore, model: &str) -> bool {
    advertised_model_ids(store)
        .iter()
        .any(|id| id.eq_ignore_ascii_case(model))
}

pub fn suggest_advertised(store: &AccountStore, model: &str, limit: usize) -> Vec<String> {
    suggest_from(advertised_model_ids(store), model, limit)
}

pub fn session_models(store: &AccountStore) -> Vec<Value> {
    models_response(store, None)
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|mut item| {
            let provider = item
                .get("owned_by")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let is_default = item
                .get("is_default")
                .cloned()
                .unwrap_or(Value::Bool(false));
            if let Some(object) = item.as_object_mut() {
                object.insert(
                    "providerLabel".to_string(),
                    Value::String(super::super::label_of(&provider)),
                );
                object.insert("provider".to_string(), Value::String(provider));
                object.insert("isDefault".to_string(), is_default);
            }
            item
        })
        .collect()
}

pub fn models_by_provider(store: &AccountStore) -> Value {
    let mut map = Map::new();
    for (kind, manifest) in active_manifests(store) {
        let mut names: Vec<_> = public_builtin_items(&[(kind, manifest)])
            .into_iter()
            .map(|(_, item)| model_id(&item))
            .collect();
        names.sort_by_key(|name| name.to_lowercase());
        map.insert(kind_id(kind).to_string(), json!(names));
    }
    for (provider, items) in super::super::custom::catalog_providers(store) {
        let mut names: Vec<_> = items.iter().map(model_id).collect();
        names.sort_by_key(|name| name.to_lowercase());
        map.insert(provider, json!(names));
    }
    // 插件家（网关 Key 的「可用模型」多选按 provider 分组读这张表）
    let rules = model_rules::current();
    for id in crate::server::core::plugin_host::installed_ids() {
        let Some(adapter) = crate::server::core::plugin_host::lookup(&id) else {
            continue;
        };
        let mut claimed = HashSet::new();
        let mut names: Vec<String> =
            public_items_for_provider(&rules, &id, &adapter.list_models(), &mut claimed)
                .into_iter()
                .map(|(_, item)| model_id(&item))
                .collect();
        names.sort_by_key(|name| name.to_lowercase());
        map.insert(id, json!(names));
    }
    Value::Object(map)
}

/// 管理视图里的一行模型（`models` 数组的一项）+ 它对应的默认映射。
///
/// ── 为什么抽成函数 ──────────────────────────────────────────
/// 内置家与插件家的清单来源完全不同（编译期枚举 vs 运行时注册表），但**行
/// 形状必须一致** —— 前端按同一套键渲染。两处各写一份的后果是插件家的行
/// 少几个键（能力位 / 可见性 / 刷新时刻），界面上表现为「这一家的列全是空」。
///
/// `provider` 是 `&str`（不是 `ProviderKind`）：插件家没有枚举值。
#[allow(clippy::too_many_arguments)]
fn push_managed_model(
    rules: &model_rules::ModelRules,
    models: &mut Vec<Value>,
    mappings: &mut Vec<Value>,
    attached: &mut HashSet<usize>,
    claimed: &mut HashSet<(String, String)>,
    provider: &str,
    source: &str,
    refreshed_at: i64,
    item: Value,
) {
    let id = model_id(&item);
    if id.is_empty() {
        return;
    }
    if !claimed.insert((provider.to_string(), id.to_lowercase())) {
        return; // 同家同 id 的重复条目（见 `manage_view` 里 claimed 的说明）
    }
    let default = rules.binding(provider, &id, &id);
    let enabled = rules.default_enabled(provider, &id);
    mappings.push(json!({
        "alias": id, "target": id, "provider": provider, "enabled": enabled,
        "reasoning": default.and_then(|mapping| mapping.reasoning.clone()),
        "isDefault": true, "dangling": false, "carried": true,
    }));
    let mut aliases = Vec::new();
    for (index, mapping) in rules.mappings.iter().enumerate() {
        if !mapping.target.eq_ignore_ascii_case(&id)
            || !mapping
                .provider
                .as_deref()
                .map_or(true, |owner| owner.eq_ignore_ascii_case(provider))
        {
            continue;
        }
        attached.insert(index);
        if mapping.alias.eq_ignore_ascii_case(&id)
            || aliases
                .iter()
                .any(|alias: &String| alias.eq_ignore_ascii_case(&mapping.alias))
        {
            continue;
        }
        let Some(effective) = rules.binding(provider, &mapping.alias, &id) else {
            continue;
        };
        aliases.push(mapping.alias.clone());
        mappings.push(json!({
            "alias": effective.alias, "target": id, "provider": provider,
            "enabled": effective.enabled, "reasoning": effective.reasoning,
            "isDefault": false, "dangling": false, "carried": true,
        }));
    }
    let source = if rules
        .custom
        .iter()
        .any(|custom| custom.matches(provider, &id))
    {
        "manual"
    } else {
        source
    };
    // 覆盖键列表从**本次快照**里查（不调 model_rules::current() —— 那会
    // 每行重新解析一遍配置，几百行的清单就是几百次解析）
    let cap_overrides: Vec<String> = rules
        .capabilities
        .iter()
        .find(|entry| entry.matches(provider, &id))
        .map(|entry| capability::overridden_keys(&entry.values))
        .unwrap_or_default();
    models.push(json!({
        "id": id, "name": item.get("name"), "credits": item.get("credits"),
        "isDefault": item.get("isDefault").and_then(Value::as_bool).unwrap_or(false),
        "provider": provider, "providerLabel": super::super::label_of(provider),
        "source": source, "enabled": enabled, "aliases": aliases,
        "capabilities": capability::effective(&item),
        "capOverrides": cap_overrides,
        // 每个能力键的**来源**（`upstream` / `modelsdev` / `user`，见
        // `core::model_meta` 的模块头）。管理页据它把「上游实测」「社区
        // 库补的」「用户改的」三档分开显示 —— 没有它，六个键看起来一样
        // 可信，而它们的可信度差着量级。缺失（没有这个键）= 那一项没有
        // 任何来源（未声明）。
        "capSources": item.get("capSources"),
        // 模型**自己能配哪些思考档位**（出口键名，由 `core::model_meta`
        // 在清单成型处归一：这家自己声明的 > models.dev 的 effort 列表）。
        // 与上面 `mappings[].reasoning`（用户给某条映射**指定**的档位）
        // 是两件事：这里回答「这个模型支持哪些档」，那里回答「这条映射
        // 用哪一档」。缺失（null）= 该模型未声明，界面据此回落通用档位表。
        "reasoningLevels": item.get("reasoningLevels"),
        "reasoningDefaultLevel": item.get("reasoningDefaultLevel"),
        // 这一家的清单是什么时候拉到的（毫秒；0 = 从未成功拉过）。
        // 缓存恢复的清单与刚拉的清单**都是 `remote`**，时效只能靠这个
        // 时间戳说明（见 `providers::catalog_cache` 的模块头）。
        "refreshedAt": refreshed_at,
        // **可见性**：这个模型在官方客户端里是否会被列出。
        //
        // `"shown"` / `"hidden"` / `"undeclared"` 三态，由各家 `models.rs`
        // 在自己的解析层算出（判据各不相同：Antigravity 按世代、Qoder 读
        // 上游 `enable`、Raccoon 读 `visible`、CodeArts 读 `display_enabled`）。
        // 聚合层不重新判断 —— 它不认识各家的规则，只负责透传。
        //
        // 缺失（`null`）= 这家没有可见性判据（当前是除上面四家之外的所有
        // provider）。界面据此显示 `—`，**不**把它当成「可见」或「不可见」：
        // 「这家没这个信息」和「这家说它不可见」是两件事。
        "visibility": item.get("visibility").cloned().unwrap_or(Value::Null),
    }));
}

/// 每条原始 ID 都返回一个默认绑定；历史的同名映射合并进默认绑定，避免两个开关
/// 控制同一个请求名。默认绑定不可删除，关闭不影响这一行的其他别名。
///
/// ── 能力位（`capabilities` / `capOverrides`）─────────────────
/// 每行带两个能力字段（管理页的「上下文 / 输出」与「能力」两列读它们）：
///   · `capabilities`：**生效值**（清单原值 + 用户覆盖，六项齐全，
///     `null` = 未声明 —— 与「不支持 / 0」区分开，用户编辑时要靠它分辨
///     「不知道」与「明确不支持」）；
///   · `capOverrides`：被用户覆盖的键（管理页据此给「已改」标记；
///     空数组 = 全部继承清单原值）。
/// 清单原值已由 `manifest_for` 应用过覆盖，所以这里读到的就是生效值。
pub fn manage_view(store: &AccountStore) -> Value {
    let rules = model_rules::current();
    let mut models = Vec::new();
    let mut mappings = Vec::new();
    let mut attached = HashSet::new();
    // `(provider, id 小写)` 的出口去重（修复 #177 的兜底一层）。
    //
    // 各家的目录解析**应**在源头去重（如 Trae 的 `parse_catalog` 按
    // `config_name`），但上游形态随时可能变化、别家未必有同样的去重 —— 管理页
    // 是「清单的可视检查」，同 id 画出两行会连带下面那条默认映射也重复一条。
    // 这一层独立于源头：不管哪家目录出了脏数据，这里只认第一次出现的那条。
    let mut claimed: HashSet<(String, String)> = HashSet::new();
    for (kind, mut manifest) in active_manifests(store) {
        let provider = kind_id(kind);
        manifest.sort_by_key(|item| !rules.default_enabled(provider, &model_id(item)));
        // 一次取「远程与否 + 拉取时刻」：两者是同一处状态，分两次取会多读一次锁
        let (remote, refreshed_at) = refresh_meta(kind);
        let source = if remote { "remote" } else { "builtin" };
        for item in manifest {
            push_managed_model(
                &rules,
                &mut models,
                &mut mappings,
                &mut attached,
                &mut claimed,
                provider,
                source,
                refreshed_at,
                item,
            );
        }
    }
    // ── 插件家（运行时装进来的那些）────────────────────────────
    // 与 `models_response` 同一理由：管理页是「清单的可视检查」，插件家的模型
    // 若不出现在这里，用户就无法启停 / 改名 / 配映射 —— 而它们已经能转发了。
    // 追加在末尾，内置家的行序（前端按 provider 匹配）逐字不变。
    //
    // ── 「有没有启用账号」这道过滤不能漏 ────────────────────────
    // 内置家走 `active_manifests` → `provider_available`（`accounts_for_provider`
    // 要求 `enabled && has_credentials`），所以「全部账号被禁用」的家在管理页
    // 里是**不出现**的。插件家若不过同一道关，装了插件之后所有家都会列出来 ——
    // 用户看到的是「模型管理里冒出十几家我根本没账号的提供商」，而内置家时代
    // 它们本来会隐藏。这正是 `models_response` 的 `append_plugin_models` 已经
    // 守着的同一条判据（见那里的注释：「没有账号时广告出去只会让客户端选到
    // 一条转发必然 503 的模型」），两处必须同源，否则「对外清单」与「管理页
    // 清单」对不上，用户没法解释为什么这里有一家、那里没有。
    for id in crate::server::core::plugin_host::installed_ids() {
        if store.accounts_for_provider(&id).is_empty() {
            continue;
        }
        let Some(adapter) = crate::server::core::plugin_host::lookup(&id) else {
            continue;
        };
        let mut manifest = adapter.list_models();
        manifest.sort_by_key(|item| !rules.default_enabled(&id, &model_id(item)));
        for item in manifest {
            push_managed_model(
                &rules,
                &mut models,
                &mut mappings,
                &mut attached,
                &mut claimed,
                &id,
                "remote",
                0,
                item,
            );
        }
    }
    for (index, mapping) in rules.mappings.iter().enumerate() {
        if attached.contains(&index) {
            continue;
        }
        let carried = match mapping.provider.as_deref() {
            Some(provider) => kind_from_id(provider).is_some_and(|kind| {
                entry_id_in_manifest(&manifest_for(kind), &mapping.target).is_some()
            }),
            None => !super::providers_for_model(&mapping.target).is_empty(),
        };
        mappings.push(json!({
            "alias": mapping.alias, "target": mapping.target, "provider": mapping.provider,
            "enabled": mapping.enabled, "reasoning": mapping.reasoning,
            "isDefault": mapping.alias.eq_ignore_ascii_case(&mapping.target),
            "dangling": true, "carried": carried,
        }));
    }
    json!({
        "models": models,
        "mappings": mappings,
        "reasoningLevels": model_rules::REASONING_LEVELS,
        // models.dev 元数据的当前状态（`builtin` / `cache` / `network` + 生成
        // 时刻 + 条数）。它服务的是「这份档位/能力是什么时候的数据」这个问题 ——
        // 界面上那些档位来自哪一份、新不新，只能由这里回答。
        "modelsDev": model_meta::status(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_raw` 收的是整份 config（它自己取 `modelRules` 键），所以包一层
    fn rules_with(mappings: Value) -> model_rules::ModelRules {
        let raw = json!({ "modelRules": { "mappings": mappings } });
        model_rules::ModelRules::from_raw(raw.as_object().expect("对象字面量"))
    }

    /// 一条展示名与别名逐字相同的上游条目（`space-bunny-alpha` 是实测命中的那个）
    fn manifest() -> Vec<Value> {
        vec![json!({ "id": "stealth/space-bunny-alpha", "name": "space-bunny-alpha" })]
    }

    fn advertised_ids(rules: &model_rules::ModelRules) -> Vec<String> {
        let mut claimed = HashSet::new();
        public_items_for_provider(rules, "cline-free", &manifest(), &mut claimed)
            .into_iter()
            .map(|(_, item)| model_id(&item))
            .collect()
    }

    /// 被关掉的映射别名不能进广告候选名。漏了这道过滤时，别名会经由
    /// `builtin_target` 的「展示名回落」绕开启停判定 —— 于是「关了映射，
    /// `/v1/models` 里却还在」（2026-10-03 实测复现）。
    #[test]
    fn disabled_mapping_alias_is_not_advertised() {
        let rules = rules_with(json!([
            { "alias": "space-bunny-alpha", "target": "stealth/space-bunny-alpha",
              "provider": "cline-free", "enabled": false }
        ]));
        let names = advertised_ids(&rules);
        assert!(
            !names.iter().any(|name| name == "space-bunny-alpha"),
            "关掉的别名不该出现在候选名里: {names:?}"
        );
        // 原生 id 不受影响（开关只管别名）
        assert!(names.iter().any(|name| name == "stealth/space-bunny-alpha"));
    }

    /// 开着的映射别名照旧进候选 —— 不能让这次修复把功能一起修没
    #[test]
    fn enabled_mapping_alias_is_advertised() {
        let rules = rules_with(json!([
            { "alias": "space-bunny-alpha", "target": "stealth/space-bunny-alpha",
              "provider": "cline-free", "enabled": true }
        ]));
        let names = advertised_ids(&rules);
        assert!(names.iter().any(|name| name == "space-bunny-alpha"));
    }

    /// 缺 `enabled` 键 = 开关功能上线前的历史条目 = 默认启用（升级不改行为）
    #[test]
    fn mapping_without_enabled_key_is_advertised() {
        let rules = rules_with(json!([
            { "alias": "space-bunny-alpha", "target": "stealth/space-bunny-alpha",
              "provider": "cline-free" }
        ]));
        let names = advertised_ids(&rules);
        assert!(names.iter().any(|name| name == "space-bunny-alpha"));
    }

    /// 别家的映射别名不该混进这一家的候选（provider 过滤仍在）
    #[test]
    fn other_providers_alias_is_not_included() {
        let rules = rules_with(json!([
            { "alias": "x-alias", "target": "stealth/space-bunny-alpha",
              "provider": "workbuddy", "enabled": true }
        ]));
        let names = advertised_ids(&rules);
        assert!(!names.iter().any(|name| name == "x-alias"));
    }

    #[test]
    fn runtime_provider_alias_is_advertised_against_its_target() {
        let rules = rules_with(json!([
            { "alias": "luna1", "target": "Qwen3.8-Flash", "provider": "qoder" }
        ]));
        let manifest = vec![json!({ "id": "Qwen3.8-Flash", "name": "Qwen 3.8 Flash" })];
        let mut claimed = HashSet::new();
        let items = public_items_for_provider(&rules, "qoder", &manifest, &mut claimed);
        let alias = items
            .iter()
            .find(|(_, item)| model_id(item) == "luna1")
            .expect("mapped alias should be advertised");
        assert_eq!(alias.1["owned_by"], "qoder");
        assert_eq!(alias.1["name"], "luna1");
    }
}
