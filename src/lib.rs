//! Kafka binding plugin for mcpg.
//!
//! Implements:
//!
//! - [`KafkaBackendPlugin`] — `BackendPlugin` for `kind: "kafka"`.
//!   Produces a request message with a unique `correlation_id` header and
//!   consumes from the configured response topic, returning the first
//!   reply whose correlation ID matches. Forwards W3C `traceparent`
//!   headers when present.
//!
//! - [`KafkaWatchPlugin`] — `WatchStrategyPlugin` for `kind: "kafka_topic"`.
//!   Spawns a consumer on the configured topic and emits a `WatchEvent`
//!   on every inbound message so resource subscribers receive
//!   `notifications/resources/updated` events.
//!
//! Unlike the NATS plugin, the binding and watch plugins each build their
//! own producer/consumer from the configured bootstrap servers. Kafka
//! consumers bind to a group, and a single consumer cannot serve both
//! unrelated correlated request/reply *and* passive topic watching
//! without offset management gymnastics — two consumers keeps semantics
//! clean.

pub mod client_registry;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest, WatchError,
    WatchEvent, WatchEventSink, WatchHandle, WatchStrategyPlugin, firstparty_manifest,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::client_registry::{
    CredDigest, IdleSweeper, KafkaClientBundle, digest_credential_bundle, spawn_idle_sweeper,
};

/// Embedded  descriptor for this plugin.
/// Passed to [`FirstPartyRegistrar::register`] at gateway startup.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

// ---------------------------------------------------------------------------
// Binding plugin — correlated request/reply
// ---------------------------------------------------------------------------

// NOTE: NO `#[serde(deny_unknown_fields)]` here. This is the per-binding
// `register_profile` spec, NOT the plugin `config:` block. The gateway
// injects a RESERVED `__mcpg_secret_refs` hint key into this spec object
// post credential-resolution (see `inject_secret_refs_hint` in the gateway
// app; this plugin reads it back via `spec.get("__mcpg_secret_refs")` in
// `register_profile` to scope rotation eviction). `deny_unknown_fields`
// would reject that injected key and break secret rotation for credentialed
// Kafka bindings — this is an intentional forward-compatible passthrough.
#[derive(Debug, Clone, Deserialize)]
struct KafkaBackendSpec {
    request_topic: String,
    response_topic: String,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_max_response_bytes")]
    max_response_bytes: usize,
    /// Optional per-binding bootstrap_servers override. When `None`,
    /// the plugin uses the constructor-provided shared
    /// bootstrap_servers.
    #[serde(default)]
    bootstrap_servers: Option<String>,
    /// Optional SASL username. A `${cred://issuer/target}` token here
    /// triggers per-caller resolution.
    #[serde(default)]
    sasl_username: Option<String>,
    /// Optional SASL password. A `${cred://issuer/target}` token here
    /// is the most common per-caller path.
    #[serde(default)]
    sasl_password: Option<String>,
    /// Optional `security.protocol` override (e.g.
    /// `SASL_SSL`, `SASL_PLAINTEXT`).
    #[serde(default)]
    security_protocol: Option<String>,
    /// Optional `sasl.mechanism` override (e.g. `SCRAM-SHA-256`,
    /// `PLAIN`).
    #[serde(default)]
    sasl_mechanism: Option<String>,
}

/// Default per-call timeout. Matches the gateway binding default
/// (`default_kafka_timeout_ms`) 1:1 so a binding that omits `timeout_ms`
/// resolves to the identical value on either path.
fn default_timeout_ms() -> u64 {
    10_000
}
/// Default response cap. Matches the gateway binding default
/// (`default_kafka_max_response_bytes` = 64 KiB) 1:1 — a binding that
/// omits `max_response_bytes` resolves to 65536 on either path.
fn default_max_response_bytes() -> usize {
    65_536
}

/// Per-profile runtime state. Cloned on every execute_inner so the
/// in-flight call can safely outlive a hot-reload that replaces
/// `profiles[backend_name]`. Heavy fields (the static `bundle`,
/// `Arc<dyn BackendHost>`, the registry, and the guard handles) are
/// behind `Arc` so cloning is cheap.
#[derive(Clone)]
struct KafkaProfileRuntime {
    request_topic: String,
    response_topic: String,
    timeout: Duration,
    max_response_bytes: usize,
    /// Snapshot of the operator's spec — kept on the runtime so the
    /// per-call resolver can re-walk the SASL fields for
    /// `${cred://…}` token substitution.
    cfg: Arc<KafkaBackendSpec>,
    /// True when the spec carries at least one `${cred://…}` token
    /// in `bootstrap_servers` / `sasl_username` / `sasl_password`.
    has_cred_refs: bool,
    /// Static-cred client bundle. Either the constructor-provided
    /// shared producer/consumer pair or, when the spec overrides
    /// connection params with values carrying no `${cred://…}` token,
    /// a per-profile pair built at register time.
    static_bundle: Arc<KafkaClientBundle>,
    /// Backend host capability — only the dynamic-cred path uses
    /// it for `resolve_credentials`.
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    /// Per-credential client cache. Static profiles never grow it.
    client_registry: Arc<client_registry::ClientRegistry>,
    /// Group id used for the per-cred consumer pairs.
    group_id: String,
    _revocation_sub: Arc<mcpg_plugin_protocol::CredentialRevocationSubscription>,
    /// Secret-rotation subscription guard. Drop = unsubscribe.
    _rotation_sub: Arc<mcpg_plugin_protocol::SecretRotationSubscription>,
    _idle_sweeper: Arc<IdleSweeper>,
}

/// `BackendPlugin` implementation for `kind: "kafka"`.
pub struct KafkaBackendPlugin {
    manifest: PluginManifest,
    /// Constructor-provided bootstrap servers — used for the
    /// static-cred fast path when the spec has no override.
    shared_bootstrap_servers: String,
    /// Shared client bundle, built lazily on first use. The cdylib
    /// factory (`from_config_json`) is infallible, so the librdkafka
    /// bundle — whose construction can fail — is deferred to the first
    /// `register_profile` that needs the static-cred fast path. `new()`
    /// (static-firstparty fast path) populates it eagerly.
    shared_bundle: Arc<std::sync::Mutex<Option<Arc<KafkaClientBundle>>>>,
    profiles: Arc<RwLock<BTreeMap<String, KafkaProfileRuntime>>>,
    group_id: String,
}

impl std::fmt::Debug for KafkaBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBackendPlugin")
            .field("group_id", &self.group_id)
            .finish()
    }
}

/// Plugin-level config (the `plugins:` entry's `config:` block). Per-
/// binding request/response topics arrive later via `register_profile`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaPluginConfig {
    #[serde(default)]
    bootstrap_servers: String,
    #[serde(default = "default_kafka_group_id")]
    group_id: String,
}

/// Matches the per-field serde defaults so the fail-closed parse helper's
/// empty/absent-config path (`T::default()`) yields the SAME value the old
/// fail-open fallback produced: empty `bootstrap_servers`, `group_id` =
/// "mcpg".
impl Default for KafkaPluginConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: default_kafka_group_id(),
        }
    }
}

fn default_kafka_group_id() -> String {
    "mcpg".to_owned()
}

impl KafkaBackendPlugin {
    /// Construct a new plugin instance, eagerly building its shared
    /// producer/consumer. Used by the static-firstparty fast path.
    pub fn new(bootstrap_servers: &str, group_id: &str) -> anyhow::Result<Self> {
        let bundle = build_kafka_bundle(bootstrap_servers, group_id, None, None, None, None)?;

        info!(bootstrap_servers = %bootstrap_servers, group_id = %group_id,
              "mcpg-plugin-backend-kafka: producer and consumer initialized");

        Ok(Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.kafka",
                name: "Kafka Binding",
                class: Backend,
            },
            shared_bootstrap_servers: bootstrap_servers.to_owned(),
            shared_bundle: Arc::new(std::sync::Mutex::new(Some(Arc::new(bundle)))),
            profiles: Arc::new(RwLock::new(BTreeMap::new())),
            group_id: group_id.to_owned(),
        })
    }

    /// Infallible cdylib factory: parse the plugin config + defer the
    /// librdkafka bundle to first use. Bad/missing config yields an
    /// instance whose first `register_profile` (static-cred path) returns
    /// a clear transport error rather than failing the plugin load.
    pub fn from_config_json(config_json: &str) -> Self {
        // Fail CLOSED: a present-but-malformed `config:` block refuses the
        // plugin (panic → null handle → host boot rejection) rather than
        // silently degrading to defaults. An empty/absent block still uses
        // `KafkaPluginConfig::default()`.
        let cfg: KafkaPluginConfig =
            mcpg_plugin_sdk::fail_closed_config!(config_json, KafkaPluginConfig);
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.kafka",
                name: "Kafka Binding",
                class: Backend,
            },
            shared_bootstrap_servers: cfg.bootstrap_servers,
            shared_bundle: Arc::new(std::sync::Mutex::new(None)),
            profiles: Arc::new(RwLock::new(BTreeMap::new())),
            group_id: cfg.group_id,
        }
    }

    /// Get the shared bundle, building + caching it on first call.
    /// Returns a transport error if the librdkafka client can't be built.
    fn shared_bundle(&self) -> Result<Arc<KafkaClientBundle>, BackendError> {
        let mut guard = self
            .shared_bundle
            .lock()
            .expect("kafka shared_bundle mutex poisoned");
        if let Some(b) = guard.as_ref() {
            return Ok(Arc::clone(b));
        }
        let bundle = build_kafka_bundle(
            &self.shared_bootstrap_servers,
            &self.group_id,
            None,
            None,
            None,
            None,
        )
        .map_err(|e| BackendError::Transport {
            message: format!("building shared Kafka client: {e}"),
        })?;
        let arc = Arc::new(bundle);
        *guard = Some(Arc::clone(&arc));
        Ok(arc)
    }
}

/// Build a `(producer, consumer)` pair from connection params. Used
/// by both the constructor (static-cred fast path) and the per-cred
/// path's registry build closure.
fn build_kafka_bundle(
    bootstrap_servers: &str,
    group_id: &str,
    sasl_username: Option<&str>,
    sasl_password: Option<&str>,
    security_protocol: Option<&str>,
    sasl_mechanism: Option<&str>,
) -> anyhow::Result<KafkaClientBundle> {
    use anyhow::Context;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::StreamConsumer;
    use rdkafka::producer::FutureProducer;

    let mut producer_cfg = ClientConfig::new();
    producer_cfg
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000");
    if let Some(p) = security_protocol {
        producer_cfg.set("security.protocol", p);
    }
    if let Some(m) = sasl_mechanism {
        producer_cfg.set("sasl.mechanism", m);
    }
    if let Some(u) = sasl_username {
        producer_cfg.set("sasl.username", u);
    }
    if let Some(p) = sasl_password {
        producer_cfg.set("sasl.password", p);
    }
    let producer: FutureProducer = producer_cfg
        .create()
        .context("failed to create Kafka producer")?;

    let mut consumer_cfg = ClientConfig::new();
    consumer_cfg
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true");
    if let Some(p) = security_protocol {
        consumer_cfg.set("security.protocol", p);
    }
    if let Some(m) = sasl_mechanism {
        consumer_cfg.set("sasl.mechanism", m);
    }
    if let Some(u) = sasl_username {
        consumer_cfg.set("sasl.username", u);
    }
    if let Some(p) = sasl_password {
        consumer_cfg.set("sasl.password", p);
    }
    let consumer: StreamConsumer = consumer_cfg
        .create()
        .context("failed to create Kafka consumer")?;

    Ok(KafkaClientBundle {
        producer,
        consumer: Arc::new(consumer),
    })
}

#[async_trait]
impl BackendPlugin for KafkaBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "kafka"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &serde_json::Value,
        host: std::sync::Arc<dyn mcpg_plugin_protocol::BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: KafkaBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("Kafka binding spec: {e}"),
            })?;

        for topic in [&parsed.request_topic, &parsed.response_topic] {
            if topic.trim().is_empty() {
                return Err(BackendError::InvalidSpec {
                    message: "request_topic and response_topic must not be empty".into(),
                });
            }
            if topic.contains('\0') || topic.len() > 249 {
                return Err(BackendError::InvalidSpec {
                    message: format!("Kafka topic '{topic}' is invalid"),
                });
            }
        }
        if parsed.timeout_ms == 0 {
            return Err(BackendError::InvalidSpec {
                message: "timeout_ms must be greater than 0".into(),
            });
        }
        if parsed.max_response_bytes == 0 {
            return Err(BackendError::InvalidSpec {
                message: "max_response_bytes must be greater than 0".into(),
            });
        }

        // `request_topic` / `response_topic` are transport-only routing
        // facts the plugin treats as plaintext (the message destination),
        // never as a credential-bearing value — a `cred://` ref there is
        // an operator mistake that would leak a resolved secret into a
        // Kafka topic name. The gateway also enforces this generically via
        // the manifest `transport_only_fields` declaration; this is the
        // owning plugin's matching reject. SASL fields are deliberately
        // EXCLUDED: `sasl_username` / `sasl_password` / `bootstrap_servers`
        // legitimately carry `${cred://issuer/target}` tokens (the
        // per-caller resolution path), so they are not transport-only.
        for (field, value) in [
            ("request_topic", parsed.request_topic.as_str()),
            ("response_topic", parsed.response_topic.as_str()),
        ] {
            if value.contains("cred://") {
                return Err(BackendError::InvalidSpec {
                    message: format!("{field} must not contain a cred:// reference"),
                });
            }
        }

        // Cross-binding connection consistency (folds the gateway's old
        // `validate_kafka_binding_consistency`): one `KafkaBackendPlugin`
        // is constructed per gateway with a single `(bootstrap_servers,
        // group_id)` from `plugins[].config`; `group_id` is never on the
        // per-binding spec, so the only divergence vector is a per-binding
        // `bootstrap_servers` override. A plaintext override that differs
        // from the configured shared brokers is a misconfiguration —
        // per-binding broker isolation is not yet supported (FUTURE.md).
        // An override carrying a `${cred://…}` token is exempt: its
        // post-resolution value is per-caller and cannot be compared at
        // register time (the dynamic-cred path requires the explicit
        // `bootstrap_servers` and resolves it per call).
        if let Some(override_bs) = parsed.bootstrap_servers.as_deref()
            && mcpg_plugin_protocol::credential::cred_tokens(override_bs).is_empty()
            && !self.shared_bootstrap_servers.is_empty()
            && override_bs != self.shared_bootstrap_servers
        {
            return Err(BackendError::InvalidSpec {
                message: format!(
                    "Kafka bindings must share connection params: binding '{backend_name}' \
                     overrides bootstrap_servers={override_bs:?}, but the plugin is \
                     configured with bootstrap_servers={:?}. Per-binding isolation is not \
                     yet supported (see FUTURE.md).",
                    self.shared_bootstrap_servers,
                ),
            });
        }

        let has_cred_refs = spec_has_cred_refs(&parsed);

        // Capture the runtime this `register_profile` is executing on so
        // the revocation/rotation callbacks — which fire LATER, outside
        // any ambient runtime when invoked across the cdylib FFI seam —
        // spawn their eviction tasks onto a known executor. On the
        // static-firstparty path this is the gateway runtime; on the
        // cdylib path it's the plugin's private `KafkaBackendCdylib`
        // runtime (the `block_on` driving this future). A plain
        // `tokio::spawn` would panic with "no reactor running" when the
        // host fires the callback off-runtime.
        let spawn_handle = tokio::runtime::Handle::current();

        // Build the static client bundle. Profiles with no `${cred://…}`
        // token and no per-binding override use the constructor's shared
        // bundle (today's behaviour). Profiles whose bootstrap_servers /
        // SASL overrides carry no `${cred://…}` token get a freshly
        // built bundle at register time. The dynamic-cred path goes
        // through `client_registry` and never uses this field.
        let static_bundle: Arc<KafkaClientBundle> = if !has_cred_refs
            && (parsed.bootstrap_servers.is_some()
                || parsed.sasl_username.is_some()
                || parsed.sasl_password.is_some()
                || parsed.security_protocol.is_some()
                || parsed.sasl_mechanism.is_some())
        {
            let bs = parsed
                .bootstrap_servers
                .as_deref()
                .unwrap_or(&self.shared_bootstrap_servers);
            let bundle = build_kafka_bundle(
                bs,
                &self.group_id,
                parsed.sasl_username.as_deref(),
                parsed.sasl_password.as_deref(),
                parsed.security_protocol.as_deref(),
                parsed.sasl_mechanism.as_deref(),
            )
            .map_err(|e| BackendError::Transport {
                message: format!("building per-binding Kafka client: {e}"),
            })?;
            Arc::new(bundle)
        } else {
            self.shared_bundle()?
        };

        let client_registry = Arc::new(client_registry::ClientRegistry::new(
            client_registry::ClientRegistryConfig::default(),
        ));

        let registry_for_cb = Arc::clone(&client_registry);
        let revocation_spawn = spawn_handle.clone();
        let revocation_sub =
            host.subscribe_credential_revoked(Arc::new(move |plugin_id: &str, target: &str| {
                let registry = Arc::clone(&registry_for_cb);
                let plugin_id = plugin_id.to_owned();
                let target = target.to_owned();
                revocation_spawn.spawn(async move {
                    let evicted = registry.evict_for(&plugin_id, &target).await;
                    if evicted > 0 {
                        tracing::info!(
                            target: "mcpg::kafka::client_registry",
                            plugin_id = %plugin_id,
                            target = %target,
                            evicted = evicted,
                            "evicted Kafka clients on credential revocation"
                        );
                    }
                });
            }));

        // Secret rotation. Same shape as the HTTP/SQL/NATS
        // subscribers — read the gateway-injected URI hint, scope
        // the eviction to those URIs.
        let rotation_secret_refs: Vec<String> = spec
            .get("__mcpg_secret_refs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let registry_for_rotation = Arc::clone(&client_registry);
        let secret_refs_for_cb: Arc<Vec<String>> = Arc::new(rotation_secret_refs);
        let rotation_spawn = spawn_handle.clone();
        let rotation_sub =
            host.subscribe_secret_rotation(Arc::new(move |secret_ref: &str, version: u64| {
                if !secret_refs_for_cb.iter().any(|r| r == secret_ref) {
                    return;
                }
                let registry = Arc::clone(&registry_for_rotation);
                let secret_ref = secret_ref.to_owned();
                rotation_spawn.spawn(async move {
                    let evicted = registry.evict_for_secret(&secret_ref).await;
                    if evicted > 0 {
                        tracing::info!(
                            target: "mcpg::kafka::client_registry",
                            secret_ref = %secret_ref,
                            version = version,
                            evicted = evicted,
                            "evicted Kafka clients on secret rotation"
                        );
                    }
                });
            }));

        let idle_sweeper = spawn_idle_sweeper(
            backend_name.to_owned(),
            Arc::clone(&client_registry),
            Duration::from_secs(60),
        );

        debug!(
            backend = %backend_name,
            request_topic = %parsed.request_topic,
            response_topic = %parsed.response_topic,
            timeout_ms = parsed.timeout_ms,
            has_cred_refs = has_cred_refs,
            "registered Kafka binding profile"
        );

        let runtime = KafkaProfileRuntime {
            request_topic: parsed.request_topic.clone(),
            response_topic: parsed.response_topic.clone(),
            timeout: Duration::from_millis(parsed.timeout_ms),
            max_response_bytes: parsed.max_response_bytes,
            cfg: Arc::new(parsed),
            has_cred_refs,
            static_bundle,
            host,
            client_registry,
            group_id: self.group_id.clone(),
            _revocation_sub: Arc::new(revocation_sub),
            _rotation_sub: Arc::new(rotation_sub),
            _idle_sweeper: idle_sweeper,
        };

        self.profiles
            .write()
            .expect("lock")
            .insert(backend_name.to_owned(), runtime);
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        // Wrap the per-call entry point in an info_span so traces
        // attribute back to this plugin and
        // operators can route traces of just `dev.mcpg.backend.
        // kafka` to a different sink via per-plugin observability
        // config. Latency lands in `mcpg_kafka_binding_call_ms`
        // so percentiles are available downstream.
        let span = info_span!(
            "kafka_binding_execute",
            plugin_id = "dev.mcpg.backend.kafka",
            backend = %backend_name,
        );
        let started = std::time::Instant::now();
        let outcome_result = Self::execute_inner(self, backend_name, request)
            .instrument(span)
            .await;
        let elapsed_ms = started.elapsed().as_millis() as f64;
        metrics::histogram!(
            "mcpg_kafka_binding_call_ms",
            "backend" => backend_name.to_owned(),
        )
        .record(elapsed_ms);
        match &outcome_result {
            Ok(_) => {
                metrics::counter!(
                    "mcpg_kafka_binding_calls_total",
                    "backend" => backend_name.to_owned(),
                    "outcome" => "ok",
                )
                .increment(1);
                debug!(backend = %backend_name, elapsed_ms = %elapsed_ms, "kafka call succeeded");
            }
            Err(e) => {
                let kind = match e {
                    BackendError::ProfileNotFound { .. } => "profile_not_found",
                    BackendError::Transport { .. } => "transport",
                    BackendError::Timeout { .. } => "timeout",
                    BackendError::InvalidSpec { .. } => "invalid_spec",
                };
                metrics::counter!(
                    "mcpg_kafka_binding_calls_total",
                    "backend" => backend_name.to_owned(),
                    "outcome" => "error",
                    "error_kind" => kind,
                )
                .increment(1);
                warn!(backend = %backend_name, error = %e, error_kind = %kind, "kafka call failed");
            }
        }
        outcome_result
    }
}

impl KafkaBackendPlugin {
    /// Inner implementation of `BackendPlugin::execute`. Split out so
    /// the trait method can wrap the call in a span + record outcome
    /// metrics without indenting the entire body.
    async fn execute_inner(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        use rdkafka::Message;
        use rdkafka::consumer::Consumer;
        use rdkafka::message::{Header, Headers, OwnedHeaders};
        use rdkafka::producer::FutureRecord;

        let profile = {
            let profiles = self.profiles.read().expect("lock");
            profiles
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };

        // Per-cred client resolution. Static profiles
        // short-circuit to the constructor-provided shared bundle;
        // dynamic-cred profiles ask the host to resolve the
        // `${cred://…}` tokens and look up / build a (producer,
        // consumer) pair from the registry keyed on the
        // resolved-credential digest.
        let bundle: Arc<KafkaClientBundle> = if profile.has_cred_refs {
            resolve_bundle_for_call(&profile, &request, backend_name).await?
        } else {
            Arc::clone(&profile.static_bundle)
        };

        let correlation_id = uuid::Uuid::new_v4().to_string();

        bundle
            .consumer
            .subscribe(&[&profile.response_topic])
            .map_err(|e| BackendError::Transport {
                message: format!("subscribe to '{}' failed: {e}", profile.response_topic),
            })?;

        let header_pairs = build_outbound_headers(&correlation_id, &request);
        let mut headers = OwnedHeaders::new();
        for (name, value) in &header_pairs {
            headers = headers.insert(Header {
                key: name.as_str(),
                value: Some(value.as_slice()),
            });
        }

        let record = FutureRecord::<str, [u8]>::to(&profile.request_topic)
            .payload(request.payload.as_slice())
            .headers(headers);

        bundle
            .producer
            .send(
                record,
                rdkafka::util::Timeout::After(Duration::from_secs(5)),
            )
            .await
            .map_err(|(err, _)| BackendError::Transport {
                message: format!("produce to '{}' failed: {err}", profile.request_topic),
            })?;

        let deadline = tokio::time::Instant::now() + profile.timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(BackendError::Timeout {
                    timeout_ms: profile.timeout.as_millis() as u64,
                });
            }

            match tokio::time::timeout(remaining, bundle.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    let matches = msg.headers().is_some_and(|headers| {
                        (0..headers.count()).any(|i| {
                            let h = headers.get(i);
                            h.key == "correlation_id" && h.value == Some(correlation_id.as_bytes())
                        })
                    });
                    if !matches {
                        continue;
                    }

                    let body = msg.payload().unwrap_or(&[]);
                    let truncated = body.len() > profile.max_response_bytes;
                    let payload = if truncated {
                        body[..profile.max_response_bytes].to_vec()
                    } else {
                        body.to_vec()
                    };
                    if truncated {
                        warn!(
                            backend = %backend_name,
                            bytes = body.len(),
                            max_bytes = profile.max_response_bytes,
                            "Kafka response truncated"
                        );
                    }
                    return Ok(BackendResponse { payload, truncated });
                }
                Ok(Err(e)) => {
                    return Err(BackendError::Transport {
                        message: format!("Kafka consume: {e}"),
                    });
                }
                Err(_) => {
                    return Err(BackendError::Timeout {
                        timeout_ms: profile.timeout.as_millis() as u64,
                    });
                }
            }
        }
    }
}

/// True when the spec's bootstrap_servers / sasl_username /
/// sasl_password carry at least one `${cred://…}` credential token.
///
/// Standardized grammar: a credential resolves ONLY when the operator
/// writes it as a `${cred://issuer/target}` token. A bare `cred://…`
/// (not wrapped in `${}`) is NOT a credential reference — it travels
/// verbatim and does NOT flip the dynamic-cred path on.
fn spec_has_cred_refs(spec: &KafkaBackendSpec) -> bool {
    let has_token = |s: &Option<String>| {
        s.as_deref()
            .is_some_and(|v| !mcpg_plugin_protocol::credential::cred_tokens(v).is_empty())
    };
    has_token(&spec.bootstrap_servers)
        || has_token(&spec.sasl_username)
        || has_token(&spec.sasl_password)
}

/// Build the outbound record-header pairs for a Kafka call, including
/// gateway-injected `correlation_id` (always), the operator/gateway-
/// supplied `request.headers` (passthrough, e.g. W3C trace context),
/// and — when the gateway threaded an idempotency hint —
/// `idempotency-key` + `idempotency-scope-hash`.
///
/// Header names are lowercase per Kafka ecosystem convention.
/// Values are UTF-8 bytes (rdkafka header values are arbitrary
/// byte arrays; we encode strings).
///
/// IMPORTANT distinction (also in the plugin's spec docstring):
/// this is APPLICATION-level idempotency, NOT Kafka producer-
/// idempotence (`enable.idempotence=true`). Producer-idempotence
/// prevents broker-side message duplication on producer retry
/// within a single producing session — useful but unrelated to the
/// cross-call replay semantics the `Idempotency-Key` contract
/// expresses. The operator controls `enable.idempotence` via the
/// Kafka client config; this propagation does NOT touch it.
fn build_outbound_headers(
    correlation_id: &str,
    request: &BackendRequest,
) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = Vec::new();
    out.push((
        "correlation_id".to_owned(),
        correlation_id.as_bytes().to_vec(),
    ));
    for (name, value) in &request.headers {
        out.push((name.clone(), value.as_bytes().to_vec()));
    }
    if let Some(hint) = request.idempotency.as_ref() {
        out.push(("idempotency-key".to_owned(), hint.key.as_bytes().to_vec()));
        out.push((
            "idempotency-scope-hash".to_owned(),
            hint.scope_hash.as_bytes().to_vec(),
        ));
    }
    out
}

/// Per-call (producer, consumer) bundle resolution under the
/// standardized `${cred://issuer/target}` grammar. Collects the
/// credential tokens the operator baked into the connection-param
/// strings, asks the host to resolve them per caller identity, then
/// substitutes each `${cred://…}` token back into its source string
/// and looks up / builds a bundle from the registry keyed on a BLAKE3
/// digest of the resolved bundle.
///
/// Only `${cred://…}` tokens resolve. A bare `cred://…` in a config
/// field is left verbatim. The token set comes from the operator
/// spec's `bootstrap_servers` / `sasl_username` / `sasl_password`
/// (config-origin by construction — Kafka has no request-arg
/// templating, so caller-controlled data never reaches the snapshot).
async fn resolve_bundle_for_call(
    profile: &KafkaProfileRuntime,
    request: &BackendRequest,
    backend_name: &str,
) -> Result<Arc<KafkaClientBundle>, BackendError> {
    use mcpg_plugin_protocol::credential::{cred_tokens, substitute_cred_tokens};

    let cfg = &profile.cfg;

    // 1. Collect the inner `cred://…` URIs from every `${cred://…}` token
    // in the connection-param strings. Bare `cred://…` is ignored.
    let mut cred_uris: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in [
        cfg.bootstrap_servers.as_deref(),
        cfg.sasl_username.as_deref(),
        cfg.sasl_password.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        for uri in cred_tokens(s) {
            cred_uris.insert(uri);
        }
    }

    // 2. Resolve those references through the host, per caller identity,
    // in one call → `uri → resolved value`. The snapshot is keyed by the
    // inner `cred://…` URI (config-origin BY CONSTRUCTION).
    let cred_map: std::collections::HashMap<String, String> = if cred_uris.is_empty() {
        std::collections::HashMap::new()
    } else {
        let mut snapshot = serde_json::Map::new();
        for uri in &cred_uris {
            snapshot.insert(uri.clone(), serde_json::Value::String(uri.clone()));
        }
        let mut snapshot = serde_json::Value::Object(snapshot);

        let mut host_ctx = mcpg_plugin_protocol::BackendInvocationContext::root(
            request.request_id.clone(),
            request.session_id.clone(),
            backend_name.to_owned(),
        );
        host_ctx.identity = request.identity.clone();
        profile
            .host
            .resolve_credentials(&host_ctx, &mut snapshot)
            .await
            .map_err(|e| match e {
                mcpg_plugin_protocol::BackendHostError::Backend { cause, .. } => cause,
                other => BackendError::Transport {
                    message: format!("credential resolution: {other}"),
                },
            })?;

        snapshot
            .as_object()
            .ok_or_else(|| BackendError::Transport {
                message: "credential resolver mutated snapshot to non-object".into(),
            })?
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
            .collect()
    };

    // 3. Substitute each `${cred://…}` token back into its source string.
    let subst = |s: &Option<String>| s.as_deref().map(|v| substitute_cred_tokens(v, &cred_map));
    let resolved_bs = subst(&cfg.bootstrap_servers);
    let resolved_user = subst(&cfg.sasl_username);
    let resolved_pass = subst(&cfg.sasl_password);

    let bs = resolved_bs
        .clone()
        .or_else(|| cfg.bootstrap_servers.clone())
        .ok_or_else(|| BackendError::InvalidSpec {
            message: "Kafka binding with ${cred://…} in SASL fields also requires \
                      `bootstrap_servers` on the spec — the per-credential path \
                      needs an explicit broker list"
                .into(),
        })?;

    // Build digest pairs from the resolved bundle.
    let mut digest_pairs: Vec<(String, String)> = Vec::with_capacity(3);
    digest_pairs.push(("bootstrap_servers".into(), bs.clone()));
    if let Some(u) = resolved_user.as_deref() {
        digest_pairs.push(("sasl_username".into(), u.to_owned()));
    }
    if let Some(p) = resolved_pass.as_deref() {
        digest_pairs.push(("sasl_password".into(), p.to_owned()));
    }
    let digest: CredDigest = digest_credential_bundle(&digest_pairs);

    // Cred-keys for revocation routing — the `(plugin_id, target)` of
    // each resolved `${cred://…}` token. Derived from the same token set
    // the snapshot was built from, so bare `cred://…` never routes.
    let mut cred_keys: Vec<(String, String)> = cred_uris
        .iter()
        .filter_map(|uri| {
            mcpg_plugin_protocol::credential::CredRef::parse(uri).map(|r| (r.plugin_id, r.target))
        })
        .collect();
    cred_keys.sort();
    cred_keys.dedup();

    let security_protocol = cfg.security_protocol.clone();
    let sasl_mechanism = cfg.sasl_mechanism.clone();
    let group_id = profile.group_id.clone();
    profile
        .client_registry
        .get_or_build(digest, cred_keys, || async move {
            build_kafka_bundle(
                &bs,
                &group_id,
                resolved_user.as_deref(),
                resolved_pass.as_deref(),
                security_protocol.as_deref(),
                sasl_mechanism.as_deref(),
            )
        })
        .await
        .map_err(|e| BackendError::Transport {
            message: format!("building per-credential Kafka client: {e}"),
        })
}

// ---------------------------------------------------------------------------
// Watch plugin — passive topic consumer for resource-change events
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaWatchSpec {
    topic: String,
    #[serde(default = "default_watch_group_id")]
    group_id: String,
}

fn default_watch_group_id() -> String {
    "mcpg-resource-watcher".to_owned()
}

/// `WatchStrategyPlugin` implementation for `kind: "kafka_topic"`.
pub struct KafkaWatchPlugin {
    manifest: PluginManifest,
    bootstrap_servers: String,
}

impl KafkaWatchPlugin {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.kafka_topic",
                name: "Kafka Topic Watch",
                class: WatchStrategy,
            },
            bootstrap_servers: bootstrap_servers.into(),
        }
    }

    /// Infallible cdylib factory. The watch consumer is built lazily in
    /// `watch()`, so construction here just stores the bootstrap servers.
    pub fn from_config_json(config_json: &str) -> Self {
        // Fail CLOSED on a malformed `config:` block (see the backend
        // factory above); empty/absent still uses the per-field defaults.
        let cfg: KafkaPluginConfig =
            mcpg_plugin_sdk::fail_closed_config!(config_json, KafkaPluginConfig);
        Self::new(cfg.bootstrap_servers)
    }
}

impl std::fmt::Debug for KafkaWatchPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaWatchPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

struct KafkaWatchHandle {
    cancel: CancellationToken,
}

#[async_trait]
impl WatchHandle for KafkaWatchHandle {
    async fn cancel(&self) {
        self.cancel.cancel();
    }
}

#[async_trait]
impl WatchStrategyPlugin for KafkaWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "kafka_topic"
    }

    async fn watch(
        &self,
        resource_uri: &str,
        spec: &serde_json::Value,
        sink: Arc<dyn WatchEventSink>,
    ) -> Result<Box<dyn WatchHandle>, WatchError> {
        use futures::StreamExt;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};

        let parsed: KafkaWatchSpec =
            serde_json::from_value(spec.clone()).map_err(|e| WatchError::InvalidSpec {
                message: format!("Kafka watch spec: {e}"),
            })?;
        if parsed.topic.trim().is_empty() {
            return Err(WatchError::InvalidSpec {
                message: "topic must not be empty".into(),
            });
        }

        // A watch only needs change-notification, not historical
        // replay: `auto.offset.reset = latest` skips the existing
        // backlog and fires on new records only, and auto-commit keeps
        // the consumer-group offset advancing without per-message
        // bookkeeping (at-most-once is acceptable — a dropped
        // notification just delays the next resolve, never corrupts).
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &parsed.group_id)
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()
            .map_err(|e| WatchError::Subscribe {
                message: format!("failed to create Kafka consumer: {e}"),
            })?;

        consumer
            .subscribe(&[&parsed.topic])
            .map_err(|e| WatchError::Subscribe {
                message: format!("subscribe to '{}': {e}", parsed.topic),
            })?;

        info!(
            uri = %resource_uri,
            topic = %parsed.topic,
            group_id = %parsed.group_id,
            "Kafka watch: consumer started"
        );

        let cancel = CancellationToken::new();
        let cancel_child = cancel.clone();
        let uri_owned = resource_uri.to_owned();
        let topic_owned = parsed.topic;

        tokio::spawn(async move {
            let mut stream = consumer.stream();
            loop {
                tokio::select! {
                    _ = cancel_child.cancelled() => {
                        debug!(uri = %uri_owned, "Kafka watch: cancelled");
                        return;
                    }
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(_)) => {
                                sink.emit(WatchEvent::default()).await;
                            }
                            Some(Err(e)) => {
                                // Transient consumer errors (rebalance,
                                // broker hiccup) — log and keep polling;
                                // rdkafka recovers the next iteration.
                                warn!(
                                    uri = %uri_owned,
                                    topic = %topic_owned,
                                    error = %e,
                                    "Kafka watch: consumer error"
                                );
                            }
                            None => {
                                // Stream exhausted — the consumer is gone
                                // and won't resume; terminate rather than
                                // spin. The host re-establishes the watch
                                // on its next resolve cycle.
                                warn!(
                                    uri = %uri_owned,
                                    topic = %topic_owned,
                                    "Kafka watch: stream ended"
                                );
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Box::new(KafkaWatchHandle { cancel }))
    }
}

// ---------------------------------------------------------------------------
// cdylib sync bridge — adapts the async `BackendPlugin` /
// `WatchStrategyPlugin` impls above onto the sync FFI traits
// (`SyncBackendPlugin` / `SyncWatchStrategyPlugin`) the cdylib vtable
// expects. Each wrapper owns a private multi-threaded tokio runtime and
// `block_on`s the async logic; the backend wrapper additionally derives an
// `Arc<dyn BackendHost>` from the `HostHandle` it receives at `make` time
// (via `HostHandleBackendHost`) so `register_profile`'s credential
// resolution + revocation/rotation subscriptions reach the gateway's real
// host services through the v31 host-FFI slots. See
// apps/gateway/docs/backend-plugin-migration/DESIGN.md.
// ---------------------------------------------------------------------------

use mcpg_plugin_sdk::ffi::{SyncBackendPlugin, SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};

/// Build a private multi-threaded runtime for a cdylib wrapper. 2 worker
/// threads cover the produce/consume round-trips + background eviction
/// tasks; rdkafka does its own I/O threading underneath, so the tokio
/// pool only drives the futures glue.
fn build_bridge_runtime(thread_name: &str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name.to_owned())
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("kafka cdylib: tokio runtime init failed: {e}"))
}

/// `SyncBackendPlugin` bridge over [`KafkaBackendPlugin`] for the cdylib
/// FFI. Holds the async plugin, a `BackendHost` derived from the
/// make-time `HostHandle`, and a private runtime.
pub struct KafkaBackendCdylib {
    inner: KafkaBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl KafkaBackendCdylib {
    /// cdylib factory: `(config_json, host_handle) -> Self`. Infallible —
    /// the inner plugin defers its librdkafka bundle to first use.
    pub fn from_host_config(config_json: &str, host: HostHandle) -> Self {
        Self {
            inner: KafkaBackendPlugin::from_config_json(config_json),
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: build_bridge_runtime("mcpg-backend-kafka"),
        }
    }
}

impl SyncBackendPlugin for KafkaBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(
        &self,
        profile_name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }
}

/// Async `WatchEventSink` that forwards each event to the cdylib FFI
/// push-callback. The sync watch trait hands the plugin a
/// `Box<dyn Fn(&str)>` that marshals a serialized `WatchEvent` back over
/// the FFI seam; this sink serializes the typed event + invokes it.
struct ClosureWatchSink {
    emit: Box<dyn Fn(&str) + Send + Sync + 'static>,
}

#[async_trait]
impl WatchEventSink for ClosureWatchSink {
    async fn emit(&self, event: WatchEvent) {
        match serde_json::to_string(&event) {
            Ok(json) => (self.emit)(&json),
            Err(e) => warn!(error = %e, "kafka watch: failed to serialize WatchEvent; dropping"),
        }
    }
}

/// Cancel state boxed behind the opaque [`WatchHandleBox`] pointer the
/// host round-trips between `watch` and `cancel`. Holds the async watch
/// handle + a runtime handle to drive its async `cancel`.
struct WatchCancelState {
    handle: Box<dyn WatchHandle>,
    rt: tokio::runtime::Handle,
}

/// `SyncWatchStrategyPlugin` bridge over [`KafkaWatchPlugin`].
pub struct KafkaWatchCdylib {
    inner: KafkaWatchPlugin,
    rt: tokio::runtime::Runtime,
}

impl KafkaWatchCdylib {
    /// cdylib factory: `(config_json, host_handle) -> Self`. The watch
    /// plugin needs no host services, so the handle is unused.
    pub fn from_host_config(config_json: &str, _host: HostHandle) -> Self {
        Self {
            inner: KafkaWatchPlugin::from_config_json(config_json),
            rt: build_bridge_runtime("mcpg-watch-kafka"),
        }
    }
}

impl SyncWatchStrategyPlugin for KafkaWatchCdylib {
    fn manifest(&self) -> &PluginManifest {
        WatchStrategyPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        WatchStrategyPlugin::kind(&self.inner)
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &serde_json::Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let sink = Arc::new(ClosureWatchSink { emit: emit_event });
        let handle = self.rt.block_on(WatchStrategyPlugin::watch(
            &self.inner,
            resource_uri,
            spec,
            sink,
        ))?;
        // Box the async handle + runtime handle, leak it to the host as
        // an opaque pointer. Reclaimed in `cancel`.
        let state = Box::new(WatchCancelState {
            handle,
            rt: self.rt.handle().clone(),
        });
        Ok(WatchHandleBox(Box::into_raw(state) as *mut ()))
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        if watch_handle.0.is_null() {
            return;
        }
        // SAFETY: the pointer was produced by `Box::into_raw` in `watch`
        // and the host round-trips it back here exactly once.
        let state = unsafe { Box::from_raw(watch_handle.0 as *mut WatchCancelState) };
        state.rt.block_on(state.handle.cancel());
    }
}

// cdylib export. The `backend` + `watch_strategy` entities are lifted
// into a single `mcpg_plugin_register` symbol the gateway's
// native_loader resolves after dlopen. Both entities live under the one
// `dev.mcpg.backend.kafka` plugin id; the watch entity carries its own
// `dev.mcpg.watch.kafka_topic` manifest at runtime via its `manifest()`
// slot and is distinguished here by `inner_name: "watch"`. The export
// symbol is gated on `cdylib-export` so the crate still builds as an
// rlib (the gateway's current static path) and in tests without
// emitting `mcpg_plugin_register`.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.kafka",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: BINDING_DESCRIPTOR_YAML,
    // No declared capabilities — matches the descriptor's
    // `required_capabilities: []` and the prior static plugin's
    // (ungated) behaviour. Capability gating for backend network
    // egress is a separate, cross-cutting change.
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    // Residual per-kind facts the gateway reads back by kind. Kafka may
    // appear as a backend pipeline step (`kind: kafka`); health is
    // advisory (Skip — Kafka liveness is tracked separately, not via an
    // active probe); the label defaults to the kind ("kafka"); no dynamic
    // tool list. `request_topic` / `response_topic` are declared
    // transport-only routing facts; this plugin's own `register_profile`
    // rejects a `cred://` ref at these positions (the gateway runs no generic
    // spec-walk over `transport_only_fields`). The SASL / bootstrap fields are
    // intentionally absent: they legitimately carry `${cred://…}` tokens.
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        pipeline_capable: true,
        transport_only_fields: ::std::vec![
            "/request_topic".to_owned(),
            "/response_topic".to_owned(),
        ],
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: KafkaBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                KafkaBackendCdylib::from_host_config(cfg, host),
        },
        watch_strategy as watch {
            inner_name: "watch",
            plugin_type: KafkaWatchCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                KafkaWatchCdylib::from_host_config(cfg, host),
        },
    ],
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_yields_defaults() {
        // Empty/absent config opts out → per-field defaults (no bootstrap
        // servers, group_id "mcpg"), not a fail-closed refusal.
        for cfg in ["", "{}", "null", "   "] {
            let plugin = KafkaBackendPlugin::from_config_json(cfg);
            assert_eq!(plugin.shared_bootstrap_servers, "");
            assert_eq!(plugin.group_id, "mcpg");
        }
    }

    #[test]
    fn valid_config_parses() {
        let plugin = KafkaBackendPlugin::from_config_json(
            r#"{"bootstrap_servers":"broker:9092","group_id":"g"}"#,
        );
        assert_eq!(plugin.shared_bootstrap_servers, "broker:9092");
        assert_eq!(plugin.group_id, "g");
    }

    #[test]
    #[should_panic(expected = "failing closed")]
    fn malformed_config_fails_closed() {
        // A present-but-unparseable config refuses the plugin (panic →
        // null handle at the FFI boundary) instead of silently defaulting.
        let _ = KafkaBackendPlugin::from_config_json("not json");
    }

    #[test]
    #[should_panic(expected = "failing closed")]
    fn watch_malformed_config_fails_closed() {
        let _ = KafkaWatchPlugin::from_config_json("not json");
    }

    #[test]
    #[should_panic(expected = "failing closed")]
    fn unknown_config_key_fails_closed() {
        // `#[serde(deny_unknown_fields)]` on `KafkaPluginConfig`: a stray /
        // typo'd key (here `bootstrap_serverz`) is a parse error, which
        // `fail_closed_config!` turns into a boot-time refusal rather than a
        // silently-ignored field that leaves `bootstrap_servers` empty.
        let _ = KafkaBackendPlugin::from_config_json(
            r#"{"bootstrap_serverz":"broker:9092","group_id":"g"}"#,
        );
    }

    #[test]
    fn binding_spec_deserializes() {
        let v = serde_json::json!({
            "request_topic": "requests",
            "response_topic": "responses",
            "timeout_ms": 5000,
            "max_response_bytes": 2048,
        });
        let spec: KafkaBackendSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.request_topic, "requests");
        assert_eq!(spec.response_topic, "responses");
        assert_eq!(spec.timeout_ms, 5000);
        assert_eq!(spec.max_response_bytes, 2048);
    }

    #[test]
    fn binding_spec_uses_defaults() {
        let v = serde_json::json!({
            "request_topic": "r",
            "response_topic": "s",
        });
        let spec: KafkaBackendSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.timeout_ms, 10_000);
        // 64 KiB — aligned 1:1 with the gateway's
        // `default_kafka_max_response_bytes`.
        assert_eq!(spec.max_response_bytes, 65_536);
    }

    #[test]
    fn watch_spec_uses_default_group_id() {
        let v = serde_json::json!({ "topic": "changes" });
        let spec: KafkaWatchSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.topic, "changes");
        assert_eq!(spec.group_id, "mcpg-resource-watcher");
    }

    #[tokio::test]
    async fn binding_plugin_kind_is_kafka() {
        let plugin =
            KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("construct plugin");
        assert_eq!(plugin.kind(), "kafka");
    }

    #[tokio::test]
    async fn watch_plugin_kind_is_kafka_topic() {
        let plugin = KafkaWatchPlugin::new("127.0.0.1:9092");
        assert_eq!(plugin.kind(), "kafka_topic");
    }

    #[tokio::test]
    async fn register_profile_rejects_empty_topics() {
        let plugin = KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("plugin");
        let err = plugin
            .register_profile(
                "bad",
                &serde_json::json!({
                    "request_topic": "",
                    "response_topic": "r",
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_profile_rejects_zero_timeout() {
        let plugin = KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("plugin");
        let err = plugin
            .register_profile(
                "bad",
                &serde_json::json!({
                    "request_topic": "a",
                    "response_topic": "b",
                    "timeout_ms": 0,
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    // --- Stage 2B conformance: the plugin is the single source of truth
    // for its defaults + value-validation + the connection-vs-per-binding
    // split + transport-only cred:// reject + the cross-binding
    // consistency rule (the checks that used to live in the gateway's
    // `KafkaBackendConfig` + `validate_kafka_binding_consistency`). ---

    /// Omitting `timeout_ms` / `max_response_bytes` resolves to the SAME
    /// defaults the gateway binding applied (10000ms / 64 KiB) — the
    /// default value is materialized by the plugin, not the gateway. The
    /// 64 KiB cap matches `default_kafka_max_response_bytes` 1:1.
    #[tokio::test]
    async fn register_profile_applies_gateway_defaults() {
        let plugin = KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("plugin");
        plugin
            .register_profile(
                "defaults",
                &serde_json::json!({
                    "request_topic": "requests",
                    "response_topic": "responses",
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .expect("registers with defaults");
        let guard = plugin.profiles.read().expect("lock");
        let profile = guard.get("defaults").expect("profile stored");
        assert_eq!(
            profile.timeout,
            Duration::from_millis(10_000),
            "timeout_ms defaults to 10000 (gateway binding default)",
        );
        assert_eq!(
            profile.max_response_bytes, 65_536,
            "max_response_bytes defaults to 64 KiB (gateway binding default)",
        );
    }

    /// An out-of-range topic (NUL byte / over 249 chars) is rejected as
    /// `InvalidSpec` (value-validation owned by the plugin).
    #[tokio::test]
    async fn register_profile_rejects_invalid_topic_value() {
        let plugin = KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("plugin");
        for bad in [
            serde_json::json!({ "request_topic": "ok", "response_topic": "x\0y" }),
            serde_json::json!({ "request_topic": "a".repeat(250), "response_topic": "ok" }),
        ] {
            let err = plugin
                .register_profile("bad", &bad, mcpg_plugin_protocol::noop_backend_host())
                .await
                .expect_err("should reject invalid topic value");
            assert!(matches!(err, BackendError::InvalidSpec { .. }));
        }
    }

    /// The connection-vs-per-binding SPLIT resolves: connection params
    /// (`bootstrap_servers` / `group_id`) come from `plugins[].config`
    /// (the constructor here), per-binding params (topics / timeout /
    /// size) come from the `register_profile` spec. A spec carrying ONLY
    /// the per-binding fields registers cleanly against the config-sourced
    /// connection — proving neither side needs the other's fields.
    #[tokio::test]
    async fn connection_vs_per_binding_split_resolves() {
        // `from_config_json` is the cdylib connection-config path the
        // gateway injects `{bootstrap_servers, group_id}` into.
        let plugin = KafkaBackendPlugin::from_config_json(
            r#"{"bootstrap_servers":"broker:9092","group_id":"shared-grp"}"#,
        );
        assert_eq!(plugin.shared_bootstrap_servers, "broker:9092");
        assert_eq!(plugin.group_id, "shared-grp");
        // Per-binding spec carries NO connection fields.
        plugin
            .register_profile(
                "split",
                &serde_json::json!({
                    "request_topic": "requests",
                    "response_topic": "responses",
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .expect("registers using config-sourced connection + spec-sourced topics");
        let guard = plugin.profiles.read().expect("lock");
        let profile = guard.get("split").expect("profile stored");
        assert_eq!(profile.request_topic, "requests");
        assert_eq!(profile.response_topic, "responses");
        // group_id is carried from the connection config, never the spec.
        assert_eq!(profile.group_id, "shared-grp");
    }

    /// Cross-binding consistency (folds the gateway's
    /// `validate_kafka_binding_consistency`): a per-binding
    /// `bootstrap_servers` override that DIFFERS from the configured
    /// shared brokers is rejected — per-binding broker isolation is not
    /// supported. An override EQUAL to the shared value is accepted.
    #[tokio::test]
    async fn register_profile_rejects_divergent_bootstrap_override() {
        let plugin = KafkaBackendPlugin::new("broker:9092", "grp").expect("plugin");
        let err = plugin
            .register_profile(
                "divergent",
                &serde_json::json!({
                    "request_topic": "r",
                    "response_topic": "s",
                    "bootstrap_servers": "other-broker:9092",
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .expect_err("divergent per-binding bootstrap_servers must be rejected");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));

        // An override equal to the shared value is fine.
        plugin
            .register_profile(
                "matching",
                &serde_json::json!({
                    "request_topic": "r",
                    "response_topic": "s",
                    "bootstrap_servers": "broker:9092",
                }),
                mcpg_plugin_protocol::noop_backend_host(),
            )
            .await
            .expect("matching per-binding bootstrap_servers must be accepted");
    }

    /// SASL / TLS secure default is preserved (R2 — no secure downgrade):
    /// omitting `security_protocol` / `sasl_mechanism` leaves them `None`
    /// so the plugin sets NO security props, exactly as the gateway path
    /// did — librdkafka's own default applies, and the plugin never forces
    /// a *weaker* protocol on the operator's behalf. The spec carries no
    /// security field that would silently downgrade an operator's intent.
    #[tokio::test]
    async fn register_profile_preserves_sasl_tls_secure_default() {
        let spec: super::KafkaBackendSpec = serde_json::from_value(serde_json::json!({
            "request_topic": "r",
            "response_topic": "s",
        }))
        .expect("spec parses");
        assert!(
            spec.security_protocol.is_none(),
            "security_protocol must default to None (no plugin-forced downgrade)"
        );
        assert!(
            spec.sasl_mechanism.is_none(),
            "sasl_mechanism must default to None (no plugin-forced downgrade)"
        );
        assert!(spec.sasl_username.is_none());
        assert!(spec.sasl_password.is_none());
    }

    /// A bare `cred://` ref in a transport-only field (`request_topic` /
    /// `response_topic`) is rejected — topic names are plaintext routing
    /// facts, never credential carriers, so a `cred://` there would leak a
    /// resolved secret into a topic name. SASL fields are NOT transport-
    /// only (they legitimately carry `${cred://…}` tokens), so the same
    /// ref in `sasl_password` is accepted.
    #[tokio::test]
    async fn register_profile_rejects_cred_in_transport_only_topic() {
        let plugin = KafkaBackendPlugin::new("broker:9092", "grp").expect("plugin");
        for bad in [
            serde_json::json!({ "request_topic": "cred://vault/topic", "response_topic": "s" }),
            serde_json::json!({ "request_topic": "r", "response_topic": "cred://vault/topic" }),
        ] {
            let err = plugin
                .register_profile("bad", &bad, mcpg_plugin_protocol::noop_backend_host())
                .await
                .expect_err("should reject cred:// in a transport-only topic field");
            assert!(matches!(err, BackendError::InvalidSpec { .. }));
        }
    }

    #[tokio::test]
    async fn execute_without_registered_profile_errors() {
        let plugin = KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-test").expect("plugin");
        let err = plugin
            .execute(
                "unknown",
                BackendRequest {
                    payload: vec![],
                    headers: vec![],
                    request_id: "r1".into(),
                    session_id: None,
                    identity: None,
                    idempotency: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::ProfileNotFound { .. }));
    }

    /// Outbound record headers MUST carry
    /// `idempotency-key` + `idempotency-scope-hash` (UTF-8 bytes,
    /// lowercase per Kafka ecosystem convention) when the gateway
    /// threaded a hint through `BackendRequest.idempotency`.
    /// Asserts on the build_outbound_headers helper directly so the
    /// test runs without a live broker (Kafka unit tests don't
    /// start one — see `binding_plugin_kind_is_kafka` etc.).
    #[test]
    fn outbound_message_carries_idempotency_headers() {
        let req = BackendRequest {
            payload: b"{}".to_vec(),
            headers: vec![],
            request_id: "r1".into(),
            session_id: None,
            identity: None,
            idempotency: Some(mcpg_plugin_protocol::IdempotencyHint {
                key: "idem-test-key".to_owned(),
                scope_hash: "deadbeefdeadbeefdeadbeefdeadbeef".to_owned(),
            }),
        };
        let pairs = super::build_outbound_headers("corr-1", &req);
        let names: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"idempotency-key"), "names: {names:?}");
        assert!(
            names.contains(&"idempotency-scope-hash"),
            "names: {names:?}"
        );
        assert!(
            names.contains(&"correlation_id"),
            "correlation_id must still be present; names: {names:?}"
        );
        let key_val = pairs
            .iter()
            .find(|(k, _)| k == "idempotency-key")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(String::from_utf8(key_val).unwrap(), "idem-test-key");
        let scope_val = pairs
            .iter()
            .find(|(k, _)| k == "idempotency-scope-hash")
            .map(|(_, v)| v.clone())
            .unwrap();
        assert_eq!(
            String::from_utf8(scope_val).unwrap(),
            "deadbeefdeadbeefdeadbeefdeadbeef"
        );
    }

    /// When no hint is set the outbound headers
    /// MUST NOT carry `idempotency-key` (no zero-value, no empty
    /// string — absent means absent).
    #[test]
    fn outbound_message_omits_idempotency_headers_without_hint() {
        let req = BackendRequest {
            payload: b"{}".to_vec(),
            headers: vec![],
            request_id: "r1".into(),
            session_id: None,
            identity: None,
            idempotency: None,
        };
        let pairs = super::build_outbound_headers("corr-1", &req);
        let names: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        assert!(
            !names.contains(&"idempotency-key"),
            "must omit when hint absent; names: {names:?}"
        );
        assert!(
            !names.contains(&"idempotency-scope-hash"),
            "must omit when hint absent; names: {names:?}"
        );
    }

    // ── cdylib sync bridge ──────────────────────────────────────────
    //
    // These exercise the `SyncBackendPlugin` / `SyncWatchStrategyPlugin`
    // wrappers the cdylib FFI invokes. The `HostHandle` is built from
    // the SDK's stub host ref (no live broker, no real host services) —
    // the subscribe slots return id 0 and resolve_credentials returns an
    // empty success, so `register_profile`'s validation + subscription
    // wiring run end-to-end through the block_on bridge.

    fn stub_host_handle() -> mcpg_plugin_sdk::HostHandle {
        // SAFETY: `stub_host_ref` returns a ref whose vtable slots are
        // 'static no-op fns + ctx 0; it outlives the handle.
        unsafe { mcpg_plugin_sdk::HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref()) }
    }

    #[test]
    fn cdylib_backend_bridge_reports_kind() {
        let plugin = KafkaBackendCdylib::from_host_config(
            &serde_json::json!({ "bootstrap_servers": "127.0.0.1:9092" }).to_string(),
            stub_host_handle(),
        );
        assert_eq!(SyncBackendPlugin::kind(&plugin), "kafka");
    }

    #[test]
    fn cdylib_watch_bridge_reports_kind() {
        let plugin = KafkaWatchCdylib::from_host_config(
            &serde_json::json!({ "bootstrap_servers": "127.0.0.1:9092" }).to_string(),
            stub_host_handle(),
        );
        assert_eq!(SyncWatchStrategyPlugin::kind(&plugin), "kafka_topic");
    }

    /// The sync `register_profile` bridge must surface the inner async
    /// plugin's `InvalidSpec` synchronously (block_on round-trip) so
    /// misconfiguration fails fast at load — no live broker needed since
    /// validation rejects before any connection attempt.
    #[test]
    fn cdylib_backend_bridge_register_profile_rejects_empty_topics() {
        let plugin = KafkaBackendCdylib::from_host_config(
            &serde_json::json!({ "bootstrap_servers": "127.0.0.1:9092" }).to_string(),
            stub_host_handle(),
        );
        let err = SyncBackendPlugin::register_profile(
            &plugin,
            "bad",
            &serde_json::json!({ "request_topic": "", "response_topic": "r" }),
        )
        .unwrap_err();
        assert!(
            matches!(err, BackendError::InvalidSpec { .. }),
            "got {err:?}"
        );
    }

    /// Confirm we don't flip
    /// `enable.idempotence` (Kafka producer-idempotence is a
    /// separate concept from application-level idempotency-key).
    /// Inspect the spec we register: it must NOT carry an
    /// `enable.idempotence` flag set by the plugin behind the
    /// operator's back. The operator controls that via Kafka
    /// client config.
    #[test]
    fn producer_idempotence_unchanged() {
        // The plugin's spec doesn't expose `enable.idempotence` —
        // it has no field for it. This is a structural assertion:
        // KafkaBackendSpec deliberately doesn't include or default
        // any setting that would flip producer-idempotence on the
        // operator's behalf. If a future commit adds such a field,
        // this test will fail to compile (catching the change).
        let spec_json = serde_json::json!({
            "request_topic": "x",
            "response_topic": "y",
            "timeout_ms": 1000,
            "max_response_bytes": 1024,
        });
        let spec: super::KafkaBackendSpec = serde_json::from_value(spec_json).unwrap();
        let serialized = serde_json::to_string(&serde_json::json!({
            "request_topic": spec.request_topic,
            "response_topic": spec.response_topic,
        }))
        .unwrap();
        assert!(
            !serialized.contains("enable.idempotence"),
            "plugin spec must not surface enable.idempotence; serialized: {serialized}"
        );
    }

    // ── SECURITY: credential-resolution snapshot is config-origin only ──
    //
    // Vulnerability class F1 (credential exfiltration): a backend
    // resolves credential references at request time by building a JSON
    // `snapshot` and calling `host.resolve_credentials(&ctx, &mut
    // snapshot)`. The host resolver substitutes ANY `cred://` string in
    // the snapshot, per caller identity, with NO config whitelist. The
    // invariant: a backend must put ONLY operator-config-origin values
    // into that snapshot — never values derived from request arguments /
    // caller-controlled data — or a malicious caller smuggles a
    // credential reference through a request-arg field and exfiltrates a
    // configured credential (for a static issuer, the secret itself).
    //
    // Kafka now follows the standardized `${cred://issuer/target}`
    // grammar: the snapshot keys are the inner `cred://…` URIs collected
    // from the `${cred://…}` tokens the operator baked into the spec's
    // `bootstrap_servers` / `sasl_username` / `sasl_password`
    // (`profile.cfg`, parsed verbatim from the binding spec in
    // `register_profile`). Kafka has NO template/CEL engine, so request
    // data (`payload` / `headers`) flows only into the outbound Kafka
    // message, never into the connection-config snapshot. This test
    // locks that invariant in: it drives the real `register_profile` →
    // `resolve_bundle_for_call` resolution seam through a `StubHost` test
    // double (no live broker) and asserts that a `cred://` and a
    // `${env.X}` smuggled through request payload + headers NEVER reach
    // the snapshot handed to the resolver, while the operator-config
    // `${cred://…}` token still resolves.

    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex as StdMutex;

    type StubCredKey = (String, String);

    /// Test `BackendHost`: resolves `cred://(plugin_id, target)` from a
    /// per-identity (`subject_id`) map — mirroring the production host's
    /// per-caller scoping — and RECORDS every snapshot it is handed plus
    /// every `(plugin_id, target)` it is asked to resolve, so the test
    /// can prove what did (and did not) reach the resolution seam.
    struct StubHost {
        /// subject_id -> (plugin_id, target) -> resolved secret.
        creds: StdMutex<StdHashMap<String, StdHashMap<StubCredKey, String>>>,
        /// Every snapshot value passed to `resolve_credentials`, captured
        /// verbatim (pre-substitution clone) for assertion.
        seen_snapshots: StdMutex<Vec<serde_json::Value>>,
        /// Every snapshot value AFTER in-place substitution — lets a test
        /// prove the resolved secret actually landed in the snapshot the
        /// backend reads back.
        resolved_snapshots: StdMutex<Vec<serde_json::Value>>,
        /// Every `(plugin_id, target)` the resolver was asked to look up.
        requested_keys: StdMutex<Vec<StubCredKey>>,
    }

    impl StubHost {
        fn new(creds: StdHashMap<String, StdHashMap<StubCredKey, String>>) -> Self {
            Self {
                creds: StdMutex::new(creds),
                seen_snapshots: StdMutex::new(Vec::new()),
                resolved_snapshots: StdMutex::new(Vec::new()),
                requested_keys: StdMutex::new(Vec::new()),
            }
        }
    }

    /// Walk `value`, replacing every `cred://plugin/target` with the
    /// mapped secret and recording each `(plugin, target)` looked up.
    /// Same `cred://` lexing as the production helpers / the http
    /// reference stub (`per_cred_resolution.rs`).
    fn stub_replace_cred_refs(
        value: &mut serde_json::Value,
        map: &StdHashMap<StubCredKey, String>,
        requested: &StdMutex<Vec<StubCredKey>>,
    ) -> usize {
        match value {
            serde_json::Value::String(s) => {
                let mut count = 0;
                while let Some(idx) = s.find("cred://") {
                    let after = &s[idx + "cred://".len()..];
                    let Some(slash) = after.find('/') else { break };
                    let plugin_id = &after[..slash];
                    let after_slash = &after[slash + 1..];
                    let end = after_slash
                        .find(|c: char| c.is_whitespace() || matches!(c, '?' | '&' | '#' | '"'))
                        .unwrap_or(after_slash.len());
                    let target = &after_slash[..end];
                    let key = (plugin_id.to_owned(), target.to_owned());
                    requested.lock().unwrap().push(key.clone());
                    if let Some(replacement) = map.get(&key) {
                        let full_uri_end = idx + "cred://".len() + slash + 1 + end;
                        s.replace_range(idx..full_uri_end, replacement);
                        count += 1;
                    } else {
                        break;
                    }
                }
                count
            }
            serde_json::Value::Array(arr) => arr
                .iter_mut()
                .map(|v| stub_replace_cred_refs(v, map, requested))
                .sum(),
            serde_json::Value::Object(obj) => obj
                .values_mut()
                .map(|v| stub_replace_cred_refs(v, map, requested))
                .sum(),
            _ => 0,
        }
    }

    #[async_trait]
    impl mcpg_plugin_protocol::BackendHost for StubHost {
        async fn invoke_tool(
            &self,
            _ctx: &mcpg_plugin_protocol::BackendInvocationContext,
            _tool_name: &str,
            _args: &serde_json::Value,
        ) -> Result<serde_json::Value, mcpg_plugin_protocol::BackendHostError> {
            Err(mcpg_plugin_protocol::BackendHostError::NotImplemented)
        }

        async fn resolve_credentials(
            &self,
            ctx: &mcpg_plugin_protocol::BackendInvocationContext,
            value: &mut serde_json::Value,
        ) -> Result<usize, mcpg_plugin_protocol::BackendHostError> {
            // Capture the snapshot exactly as the backend handed it to us,
            // BEFORE substitution — this is the security boundary under test.
            self.seen_snapshots.lock().unwrap().push(value.clone());

            // Production host scopes resolution by caller identity; the
            // Kafka path sets `ctx.identity = request.identity`.
            let principal = ctx
                .identity
                .as_ref()
                .and_then(|i| i.subject_id.clone())
                .unwrap_or_default();
            let creds = self.creds.lock().unwrap();
            let map = creds.get(&principal).cloned().unwrap_or_default();
            let n = stub_replace_cred_refs(value, &map, &self.requested_keys);
            // Capture the post-substitution snapshot too, so a test can
            // prove the resolved secret actually landed in place.
            self.resolved_snapshots.lock().unwrap().push(value.clone());
            Ok(n)
        }

        fn subscribe_credential_revoked(
            &self,
            _cb: mcpg_plugin_protocol::CredentialRevocationCallback,
        ) -> mcpg_plugin_protocol::CredentialRevocationSubscription {
            mcpg_plugin_protocol::CredentialRevocationSubscription::noop()
        }

        fn subscribe_secret_rotation(
            &self,
            _cb: mcpg_plugin_protocol::SecretRotationCallback,
        ) -> mcpg_plugin_protocol::SecretRotationSubscription {
            mcpg_plugin_protocol::SecretRotationSubscription::noop()
        }
    }

    fn verified_identity(subject: &str) -> mcpg_plugin_protocol::PluginIdentity {
        mcpg_plugin_protocol::PluginIdentity {
            kind: "verified".to_owned(),
            trust_level: "verified".to_owned(),
            subject_id: Some(subject.to_owned()),
            auth_provider: None,
            issuer: None,
            roles: Vec::new(),
            groups: Vec::new(),
            scopes: Vec::new(),
            attributes: std::collections::BTreeMap::new(),
        }
    }

    /// SECURITY (F1 — credential exfiltration via request-arg-smuggled
    /// `cred://`): a caller-controlled `cred://attacker/db-password`
    /// placed in the request `payload` + `headers` MUST NEVER reach the
    /// credential-resolution snapshot, even though the host *could*
    /// resolve it (its presence in the per-identity map proves
    /// resolution would succeed). The operator-written
    /// `sasl_password: ${cred://static-broker/sasl-pw}` MUST still
    /// resolve.
    ///
    /// We drive the real `register_profile` → `resolve_bundle_for_call`
    /// seam (the latter called directly off the registered profile so no
    /// live broker is touched — `build_kafka_bundle` constructs the
    /// rdkafka clients without connecting, exactly as the existing
    /// no-broker unit tests rely on) and assert on the snapshot the
    /// `StubHost` was handed.
    #[tokio::test]
    async fn request_injected_cred_uri_never_reaches_resolution_snapshot() {
        const OPERATOR_SASL_SECRET: &str = "REAL_SASL_SECRET";
        const ATTACKER_DB_SECRET: &str = "SUPER_SECRET_DB_PW";
        const INJECTED_CRED: &str = "cred://attacker/db-password";
        const INJECTED_ENV: &str = "${env.MCPG_KAFKA_SECTEST_LEAK}";

        // Per-identity map knows BOTH the operator's cred and the
        // attacker's cred, so resolution WOULD succeed for the injected
        // ref if it ever reached the snapshot.
        let mut per_identity = StdHashMap::new();
        let mut mallory = StdHashMap::new();
        mallory.insert(
            ("static-broker".to_owned(), "sasl-pw".to_owned()),
            OPERATOR_SASL_SECRET.to_owned(),
        );
        mallory.insert(
            ("attacker".to_owned(), "db-password".to_owned()),
            ATTACKER_DB_SECRET.to_owned(),
        );
        per_identity.insert("mallory".to_owned(), mallory);

        // Hold the concrete Arc for post-call inspection; hand a
        // type-erased clone to `register_profile`.
        let stub = Arc::new(StubHost::new(per_identity));
        let host: Arc<dyn mcpg_plugin_protocol::BackendHost> = Arc::clone(&stub) as _;

        let plugin =
            KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-sectest").expect("construct plugin");

        // Operator spec: a `${cred://…}` token in sasl_password drives
        // the dynamic-cred path (has_cred_refs = true). An explicit
        // bootstrap_servers is required for that path (see InvalidSpec
        // branch).
        let spec = serde_json::json!({
            "request_topic": "requests",
            "response_topic": "responses",
            "bootstrap_servers": "127.0.0.1:9092",
            "sasl_username": "svc-user",
            "sasl_password": "${cred://static-broker/sasl-pw}",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
        });
        plugin
            .register_profile("sec", &spec, host)
            .await
            .expect("register dynamic-cred profile");

        // Pull the registered runtime and drive the resolution seam
        // directly — avoids the post-resolution broker produce/recv I/O
        // while exercising the exact snapshot-construction path.
        let profile = plugin
            .profiles
            .read()
            .expect("lock")
            .get("sec")
            .cloned()
            .expect("profile registered");
        assert!(
            profile.has_cred_refs,
            "operator ${{cred://…}} token in sasl_password must set has_cred_refs"
        );

        // Malicious request: smuggle a resolvable `cred://` and a
        // `${env.X}` through BOTH the payload (tool args) and the
        // propagated headers — every caller-controlled surface Kafka sees.
        let request = BackendRequest {
            payload: serde_json::to_vec(&serde_json::json!({
                "evil_cred": INJECTED_CRED,
                "evil_env": INJECTED_ENV,
            }))
            .unwrap(),
            headers: vec![
                ("x-evil-cred".to_owned(), INJECTED_CRED.to_owned()),
                ("x-evil-env".to_owned(), INJECTED_ENV.to_owned()),
            ],
            request_id: "req-mallory".to_owned(),
            session_id: None,
            identity: Some(verified_identity("mallory")),
            idempotency: None,
        };

        let bundle = super::resolve_bundle_for_call(&profile, &request, "sec")
            .await
            .expect("resolution should succeed (operator ${cred://…} resolves)");
        // The bundle built — operator ${cred://…} resolved to a real secret.
        drop(bundle);

        // Inspect what the resolver was handed via the retained Arc.
        let snapshots = stub.seen_snapshots.lock().unwrap().clone();
        let requested = stub.requested_keys.lock().unwrap().clone();

        assert_eq!(
            snapshots.len(),
            1,
            "resolve_credentials called exactly once"
        );
        let snapshot = &snapshots[0];
        let snap_str = serde_json::to_string(snapshot).unwrap();

        // 1. The inner cred URI from the operator's `${cred://…}` token IS
        //    present in the snapshot (so it resolves) — proves the
        //    resolution path actually ran. Under the standardized grammar
        //    the snapshot is keyed by the inner `cred://…` URI, NOT by the
        //    config field name.
        let snap_pw = snapshot
            .get("cred://static-broker/sasl-pw")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert_eq!(
            snap_pw, "cred://static-broker/sasl-pw",
            "operator-config ${{cred://…}} token must be presented to the resolver; snapshot: {snap_str}"
        );
        assert!(
            requested.contains(&("static-broker".to_owned(), "sasl-pw".to_owned())),
            "operator ${{cred://…}} token must be resolved; requested keys: {requested:?}"
        );

        // 2. SECURITY: NO caller-controlled data reached the snapshot.
        assert!(
            !snap_str.contains(INJECTED_CRED),
            "SECURITY: request-injected cred:// leaked into the resolution snapshot: {snap_str}"
        );
        assert!(
            !snap_str.contains(ATTACKER_DB_SECRET),
            "SECURITY: attacker secret resolved into the snapshot/connection: {snap_str}"
        );
        assert!(
            !snap_str.contains(INJECTED_ENV) && !snap_str.contains("MCPG_KAFKA_SECTEST_LEAK"),
            "SECURITY: request-injected ${{$env.X}} leaked into the resolution snapshot: {snap_str}"
        );
        assert!(
            !requested.contains(&("attacker".to_owned(), "db-password".to_owned())),
            "SECURITY: resolver was asked to resolve the request-injected cred:// — \
             caller-controlled data reached the snapshot; requested keys: {requested:?}"
        );
    }

    /// GRAMMAR (positive): a `${cred://issuer/target}` token in
    /// sasl_password resolves at request time. Drives the real
    /// `register_profile` → `resolve_bundle_for_call` seam against a
    /// `StubHost` (no live broker — `build_kafka_bundle` constructs the
    /// rdkafka clients without connecting) and asserts the resolver was
    /// handed the inner `cred://…` URI and asked to resolve its
    /// `(issuer, target)`.
    #[tokio::test]
    async fn wrapped_cred_token_resolves_at_request_time() {
        const SECRET: &str = "RESOLVED_SASL_SECRET";

        let mut per_identity = StdHashMap::new();
        let mut alice = StdHashMap::new();
        alice.insert(
            ("static-broker".to_owned(), "sasl-pw".to_owned()),
            SECRET.to_owned(),
        );
        per_identity.insert("alice".to_owned(), alice);

        let stub = Arc::new(StubHost::new(per_identity));
        let host: Arc<dyn mcpg_plugin_protocol::BackendHost> = Arc::clone(&stub) as _;

        let plugin =
            KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-grammar").expect("construct plugin");
        let spec = serde_json::json!({
            "request_topic": "requests",
            "response_topic": "responses",
            "bootstrap_servers": "127.0.0.1:9092",
            "sasl_username": "svc-user",
            "sasl_password": "${cred://static-broker/sasl-pw}",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
        });
        plugin
            .register_profile("g", &spec, host)
            .await
            .expect("register dynamic-cred profile");

        let profile = plugin
            .profiles
            .read()
            .expect("lock")
            .get("g")
            .cloned()
            .expect("profile registered");
        assert!(
            profile.has_cred_refs,
            "${{cred://…}} token must flip the dynamic-cred path on"
        );

        let request = BackendRequest {
            payload: Vec::new(),
            headers: Vec::new(),
            request_id: "req-alice".to_owned(),
            session_id: None,
            identity: Some(verified_identity("alice")),
            idempotency: None,
        };
        let bundle = super::resolve_bundle_for_call(&profile, &request, "g")
            .await
            .expect("resolution should succeed");
        drop(bundle);

        let seen = stub.seen_snapshots.lock().unwrap().clone();
        let resolved = stub.resolved_snapshots.lock().unwrap().clone();
        let requested = stub.requested_keys.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "resolver called exactly once");

        // The snapshot the resolver was handed (captured pre-substitution)
        // is keyed by the inner cred URI under the standardized grammar —
        // proving the `${cred://…}` token was collected and presented.
        let presented = seen[0]
            .get("cred://static-broker/sasl-pw")
            .and_then(serde_json::Value::as_str);
        assert_eq!(
            presented,
            Some("cred://static-broker/sasl-pw"),
            "${{cred://…}} token's inner URI must be presented to the resolver; snapshot: {:?}",
            seen[0]
        );

        // After resolution the inner URI is substituted to the issuer's
        // secret in place — this is the value the backend reads back and
        // feeds into the connection params.
        let landed = resolved[0]
            .get("cred://static-broker/sasl-pw")
            .and_then(serde_json::Value::as_str);
        assert_eq!(
            landed,
            Some(SECRET),
            "${{cred://…}} token must resolve to the issuer's secret; resolved snapshot: {:?}",
            resolved[0]
        );
        assert!(
            requested.contains(&("static-broker".to_owned(), "sasl-pw".to_owned())),
            "resolver must be asked for the token's (issuer, target); requested: {requested:?}"
        );
    }

    /// GRAMMAR (negative): a BARE `cred://…` (not wrapped in `${}`) in a
    /// config field is NO LONGER a credential reference under the
    /// standardized grammar. It must NOT flip the dynamic-cred path on —
    /// the profile registers as a static profile and the bare string
    /// travels verbatim into the connection params, never resolved.
    #[tokio::test]
    async fn bare_cred_uri_in_config_is_left_verbatim() {
        // Unit contract the kafka path relies on: a bare cred:// is not a
        // token, so it neither flips `spec_has_cred_refs` nor contributes
        // a cred URI to the resolution snapshot.
        let bare_spec: super::KafkaBackendSpec = serde_json::from_value(serde_json::json!({
            "request_topic": "r",
            "response_topic": "s",
            "bootstrap_servers": "127.0.0.1:9092",
            "sasl_password": "cred://static-broker/sasl-pw",
        }))
        .unwrap();
        assert!(
            !super::spec_has_cred_refs(&bare_spec),
            "a BARE cred:// must NOT be treated as a credential reference"
        );

        // A `StubHost` that would HAPPILY resolve the bare URI if it were
        // ever handed it — proving the static path never calls it.
        let mut per_identity = StdHashMap::new();
        let mut bob = StdHashMap::new();
        bob.insert(
            ("static-broker".to_owned(), "sasl-pw".to_owned()),
            "SECRET_THAT_MUST_NOT_BE_FETCHED".to_owned(),
        );
        per_identity.insert("bob".to_owned(), bob);
        let stub = Arc::new(StubHost::new(per_identity));
        let host: Arc<dyn mcpg_plugin_protocol::BackendHost> = Arc::clone(&stub) as _;

        let plugin =
            KafkaBackendPlugin::new("127.0.0.1:9092", "mcpg-bare").expect("construct plugin");
        let spec = serde_json::json!({
            "request_topic": "requests",
            "response_topic": "responses",
            "bootstrap_servers": "127.0.0.1:9092",
            "sasl_username": "svc-user",
            "sasl_password": "cred://static-broker/sasl-pw",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
        });
        // Registers fine: the bare cred:// is just a literal SASL password
        // string, baked into a static client bundle at register time.
        plugin
            .register_profile("bare", &spec, host)
            .await
            .expect("register static profile (bare cred:// is a literal)");

        let profile = plugin
            .profiles
            .read()
            .expect("lock")
            .get("bare")
            .cloned()
            .expect("profile registered");
        assert!(
            !profile.has_cred_refs,
            "a BARE cred:// must take the static path (has_cred_refs = false)"
        );

        // The static path was taken at register time → the resolver was
        // NEVER called, so the bare cred:// could not have been resolved.
        assert!(
            stub.seen_snapshots.lock().unwrap().is_empty(),
            "resolve_credentials must NOT be called for a bare cred:// profile"
        );
        assert!(
            stub.requested_keys.lock().unwrap().is_empty(),
            "resolver must NOT be asked to resolve a bare cred://"
        );
    }
}
