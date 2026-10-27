//! 对外模型名到上游模型的解析。路由与发送侧共用本解析，避免选中一家后再换一套规则。

use serde_json::Value;

use crate::server::core::{custom_providers, model_rules};
use crate::server::core::models::model_id;
use crate::server::core::providers::adapter::adapter_by_id;

use super::{all_provider_ids, manifest_for_provider};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WireTarget {
    pub model: String,
    pub reasoning: Option<String>,
}

pub(super) fn entry_id_in_manifest(manifest: &[Value], requested: &str) -> Option<String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }
    // 保留展示名兼容，但必须先匹配原始 ID；返回清单里的实际大小写。
    manifest.iter().find(|entry| model_id(entry).eq_ignore_ascii_case(requested))
        .or_else(|| manifest.iter().find(|entry| {
            entry.get("name")
                .map(crate::server::core::models::shape_value_text)
                .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        }))
        .map(model_id)
}

pub(super) fn builtin_target(
    rules: &model_rules::ModelRules,
    provider: &str,
    manifest: &[Value],
    requested: &str,
) -> Option<WireTarget> {
    target_candidates_for_provider(rules, provider, manifest, requested)
        .into_iter()
        .next()
}

fn target_candidates_for_provider(
    rules: &model_rules::ModelRules,
    provider: &str,
    manifest: &[Value],
    requested: &str,
) -> Vec<WireTarget> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Vec::new();
    }
    let exact = manifest.iter()
        .find(|entry| model_id(entry).eq_ignore_ascii_case(requested))
        .map(model_id);
    let mappings: Vec<_> = rules.mappings.iter()
        .filter(|mapping| mapping.alias.eq_ignore_ascii_case(requested))
        .filter(|mapping| mapping.provider.as_deref()
            .map_or(true, |id| id.eq_ignore_ascii_case(provider)))
        .collect();
    let default = exact.or_else(|| {
        if mappings.is_empty() { entry_id_in_manifest(manifest, requested) } else { None }
    });
    if let Some(id) = default {
        if rules.default_enabled(provider, &id) {
            return vec![WireTarget {
                reasoning: rules.binding(provider, &id, &id).and_then(|m| m.reasoning.clone()),
                model: id,
            }];
        }
    }
    let candidates: Vec<_> = mappings.into_iter()
        .filter(|mapping| !mapping.alias.eq_ignore_ascii_case(&mapping.target))
        .filter(|mapping| rules.binding(provider, &mapping.alias, &mapping.target)
            .is_some_and(|effective| effective.enabled))
        .filter_map(|mapping| {
            entry_id_in_manifest(manifest, &mapping.target).map(|target| (mapping, target))
        })
        .collect();
    let prefix = super::super::cline::models::Pool::from_provider_id(provider)
        .map(super::super::cline::models::Pool::target_prefix);
    let mut prioritized = Vec::with_capacity(candidates.len());
    let mut rest = Vec::with_capacity(candidates.len());
    let mut inherited = Vec::new();
    for (mapping, target) in candidates {
        match (
            mapping.provider.is_some(),
            prefix.is_some_and(|prefix| target.starts_with(prefix)),
        ) {
            (true, true) => prioritized.push((mapping, target)),
            (true, false) => rest.push((mapping, target)),
            (false, _) => inherited.push((mapping, target)),
        }
    }
    prioritized.extend(rest);
    prioritized.extend(inherited);
    prioritized
        .into_iter()
        .map(|(mapping, target)| WireTarget {
            model: target.clone(),
            reasoning: rules.binding(provider, &mapping.alias, &mapping.target)
                .and_then(|effective| effective.reasoning.clone()),
        })
        .collect()
}

/// 原始能力查询不应用开关，供管理 API 展开旧版全局规则。
/// 别名也算登记过的名字，但必须能解析到该家的真实上游记录。
pub fn providers_for_model(model: &str) -> Vec<String> {
    let model = model.trim();
    if model.is_empty() {
        return Vec::new();
    }
    let rules = model_rules::current();
    let manifests: Vec<_> = all_provider_ids().into_iter()
        .map(|provider| {
            let manifest = manifest_for_provider(&provider);
            (provider, manifest)
        })
        .collect();
    let exact = manifests.iter().any(|(_, items)| {
        items.iter().any(|item| model_id(item).eq_ignore_ascii_case(model))
    });
    let mut result: Vec<_> = manifests.into_iter().filter(|(provider, items)| {
        let declared = if exact {
            items.iter().any(|item| model_id(item).eq_ignore_ascii_case(model))
        } else {
            entry_id_in_manifest(items, model).is_some()
        };
        let mapped = rules.mappings.iter().any(|mapping| {
            mapping.alias.eq_ignore_ascii_case(model)
                && mapping.provider.as_deref().map_or(true, |owner| owner.eq_ignore_ascii_case(provider))
                && entry_id_in_manifest(items, &mapping.target).is_some()
        });
        declared || mapped
    }).map(|(provider, _)| provider).collect();
    result.extend(custom_providers::list().into_iter().filter_map(|provider| {
        let models = provider.get("models")?.as_array()?;
        let declared = models.iter().any(|entry| model_id(entry).eq_ignore_ascii_case(model));
        let mapped = provider
            .get("mappings")
            .and_then(Value::as_array)
            .is_some_and(|mappings| {
                mappings.iter().any(|mapping| {
                    mapping
                        .get("alias")
                        .and_then(Value::as_str)
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(model))
                })
            });
        (declared || mapped)
            .then(|| provider.get("id").and_then(Value::as_str).unwrap_or("").to_string())
    }));
    result
}

/// 只返回有开启绑定的提供商，不把关闭的 ID 回落给默认提供商。
pub fn forwarding_providers(model: &str) -> Vec<String> {
    let rules = model_rules::current();
    let mut result: Vec<_> = all_provider_ids().into_iter().filter(|provider| {
        builtin_target(&rules, provider, &manifest_for_provider(provider), model).is_some()
    }).collect();
    result.extend(custom_providers::carriers_of_model(model));
    result
}

pub fn model_blocked_everywhere(model: &str) -> bool {
    if !forwarding_providers(model).is_empty() {
        return false;
    }
    !providers_for_model(model).is_empty()
        || model_rules::current().has_alias(model)
        || custom_providers::list().iter().any(|provider| {
            provider.get("mappings").and_then(Value::as_array).is_some_and(|mappings| {
                mappings.iter().any(|mapping| mapping.get("alias").and_then(Value::as_str)
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(model)))
            })
        })
}

pub fn wire_target_for_provider(model: &str, provider_id: &str, _account: Option<&Value>) -> WireTarget {
    let requested = model.trim();
    // 自定义提供商在自己的转发模块解析一次，避免把别名提前改写后丢失其等级绑定。
    let resolved = if custom_providers::is_custom_provider_id(provider_id) {
        None
    } else {
        builtin_target(&model_rules::current(), provider_id, &manifest_for_provider(provider_id), requested)
    };
    resolved.unwrap_or_else(|| WireTarget { model: requested.to_string(), reasoning: None })
}

pub fn wire_targets_for_provider(
    model: &str,
    provider_id: &str,
    _account: Option<&Value>,
) -> Vec<WireTarget> {
    let requested = model.trim();
    if requested.is_empty() || custom_providers::is_custom_provider_id(provider_id) {
        return Vec::new();
    }
    target_candidates_for_provider(
        &model_rules::current(),
        provider_id,
        &manifest_for_provider(provider_id),
        requested,
    )
}

pub fn default_model_catalog() -> Vec<Value> {
    let rules = model_rules::current();
    let mut result = Vec::new();
    for provider in all_provider_ids().into_iter().filter(|provider| {
        adapter_by_id(provider).is_some_and(|adapter| adapter.supports_default_model())
    }) {
        for entry in manifest_for_provider(&provider) {
            let id = model_id(&entry);
            if !id.is_empty() && !super::super::images::model_is_image(&entry)
                && rules.default_enabled(&provider, &id)
                && !result.iter().any(|item| model_id(item).eq_ignore_ascii_case(&id))
            {
                result.push(entry);
            }
        }
    }
    result
}

pub fn default_model_usable(model: &str) -> bool {
    entry_id_in_manifest(&default_model_catalog(), model).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::core::model_rules::ModelRules;
    use serde_json::json;

    /// `from_raw` 收的是整份 config（它自己取 `modelRules` 键），所以包一层。
    fn rules_with(mappings: Value) -> ModelRules {
        let raw = json!({ "modelRules": { "mappings": mappings } });
        ModelRules::from_raw(raw.as_object().expect("对象字面量"))
    }

    /// 一家的最小清单：两个上游模型。
    fn manifest() -> Vec<Value> {
        vec![
            json!({ "id": "model-a", "name": "Model A" }),
            json!({ "id": "model-b", "name": "Model B" }),
        ]
    }

    fn targets(rules: &ModelRules, requested: &str) -> Vec<String> {
        target_candidates_for_provider(rules, "workbuddy", &manifest(), requested)
            .into_iter()
            .map(|wire| wire.model)
            .collect()
    }

    /// 家内主备：同一 alias 在同一家挂多条映射时全部进链，组内保持配置顺序。
    #[test]
    fn same_alias_in_one_family_becomes_an_ordered_chain() {
        let rules = rules_with(json!([
            { "alias": "free", "target": "model-a", "provider": "workbuddy", "enabled": true },
            { "alias": "free", "target": "model-b", "provider": "workbuddy", "enabled": true },
        ]));
        assert_eq!(targets(&rules, "free"), vec!["model-a", "model-b"]);
    }

    /// 原生承载独占整条链：请求名命中清单 id 时映射不摊平原生结果。
    #[test]
    fn native_route_excludes_mappings() {
        let rules = rules_with(json!([
            { "alias": "model-a", "target": "model-b", "provider": "workbuddy", "enabled": true },
        ]));
        assert_eq!(targets(&rules, "model-a"), vec!["model-a"]);
        assert_eq!(targets(&rules, "model-b"), vec!["model-b"]);
    }

    /// 关闭的映射一条都不进链。
    #[test]
    fn disabled_mapping_is_excluded() {
        let rules = rules_with(json!([
            { "alias": "free", "target": "model-a", "provider": "workbuddy", "enabled": false },
            { "alias": "free", "target": "model-b", "provider": "workbuddy", "enabled": true },
        ]));
        assert_eq!(targets(&rules, "free"), vec!["model-b"]);
    }

    /// 组内顺序是配置顺序，不是字典序。
    #[test]
    fn mapping_order_is_config_order_not_lexicographic() {
        let rules = rules_with(json!([
            { "alias": "free", "target": "zeta", "provider": "workbuddy", "enabled": true },
            { "alias": "free", "target": "alpha", "provider": "workbuddy", "enabled": true },
        ]));
        let mut manifest = manifest();
        manifest.push(json!({ "id": "zeta", "name": "Zeta" }));
        manifest.push(json!({ "id": "alpha", "name": "Alpha" }));
        let got: Vec<String> =
            target_candidates_for_provider(&rules, "workbuddy", &manifest, "free")
                .into_iter()
                .map(|wire| wire.model)
                .collect();
        assert_eq!(got, vec!["zeta", "alpha"]);
    }

    /// Cline 池前缀优先：带本池通道前缀的 target 排在链首，其余按原序跟随。
    #[test]
    fn cline_pool_prefix_is_preferred_then_rest_follow_in_order() {
        let rules = rules_with(json!([
            { "alias": "glm", "target": "z-ai/glm-5.3-flash", "provider": "cline-free", "enabled": true },
            { "alias": "glm", "target": "cline-free/glm-5.3-flash", "provider": "cline-free", "enabled": true },
        ]));
        let manifest = vec![
            json!({ "id": "cline-free/glm-5.3-flash", "name": "Cline Free GLM Flash" }),
            json!({ "id": "z-ai/glm-5.3-flash", "name": "Z AI GLM Flash" }),
        ];
        let got: Vec<String> = target_candidates_for_provider(&rules, "cline-free", &manifest, "glm")
            .into_iter()
            .map(|wire| wire.model)
            .collect();
        assert_eq!(got, vec!["cline-free/glm-5.3-flash", "z-ai/glm-5.3-flash"]);
    }

    /// 解析不出任何目标（未知名字、空请求名）→ 空表。
    #[test]
    fn unresolvable_name_yields_empty_chain() {
        let rules = rules_with(json!([]));
        assert!(targets(&rules, "unknown-model").is_empty());
        assert!(target_candidates_for_provider(&rules, "workbuddy", &manifest(), "").is_empty());
    }

    /// 别家挂的同名映射不进这一家的链。
    #[test]
    fn other_family_mapping_is_not_in_this_chain() {
        let rules = rules_with(json!([
            { "alias": "free", "target": "model-a", "provider": "raccoon", "enabled": true },
        ]));
        assert!(targets(&rules, "free").is_empty());
    }

    #[test]
    fn runtime_provider_mapping_resolves_alias_to_canonical_manifest_id() {
        let manifest = vec![json!({ "id": "Qwen3.8-Flash", "name": "Qwen 3.8 Flash" })];
        let rules = rules_with(json!([
            { "alias": "luna1", "target": "qwen3.8-flash", "provider": "qoder" }
        ]));
        let chain = target_candidates_for_provider(&rules, "qoder", &manifest, "luna1");
        assert_eq!(
            chain,
            vec![WireTarget {
                model: "Qwen3.8-Flash".into(),
                reasoning: None
            }]
        );
    }

    #[test]
    fn runtime_provider_mapping_rejects_disabled_or_unavailable_targets() {
        let manifest = vec![json!({ "id": "Qwen3.8-Flash" })];
        let disabled = rules_with(json!([
            { "alias": "luna1", "target": "Qwen3.8-Flash", "provider": "qoder", "enabled": false }
        ]));
        assert!(target_candidates_for_provider(&disabled, "qoder", &manifest, "luna1").is_empty());

        let unavailable = rules_with(json!([
            { "alias": "luna1", "target": "not-in-catalog", "provider": "qoder" }
        ]));
        assert!(
            target_candidates_for_provider(&unavailable, "qoder", &manifest, "luna1").is_empty()
        );
    }

    #[test]
    fn runtime_provider_mapping_keeps_provider_isolation() {
        let manifest = vec![json!({ "id": "Qwen3.8-Flash" })];
        let rules = rules_with(json!([
            { "alias": "luna1", "target": "Qwen3.8-Flash", "provider": "qoder-intl" }
        ]));
        assert!(target_candidates_for_provider(&rules, "qoder", &manifest, "luna1").is_empty());
    }
}
