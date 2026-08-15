//! Per-credential Kafka client cache.
//!
//! Each entry holds a `(FutureProducer, StreamConsumer)` bundle —
//! Kafka SASL credentials are per-connection, so a per-caller
//! credential set needs its own producer + consumer pair. The
//! consumer's `group.id` is taken from the binding spec (so all
//! callers share the same response-topic group) but the connection
//! itself authenticates with the resolved credentials.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, info};

/// 32-byte BLAKE3 digest of the resolved credential bundle.
pub type CredDigest = [u8; 32];

/// `BLAKE3("static")` — the digest used by the static-cred fast
/// path so a profile with no `cred://` references gets exactly one
/// (producer, consumer) pair through the registry.
#[must_use]
pub fn static_digest() -> CredDigest {
    blake3::hash(b"static").into()
}

/// Stable digest of a resolved credential bundle. Pairs are sorted
/// before hashing so call-site key order doesn't shift the digest.
#[must_use]
pub fn digest_credential_bundle(pairs: &[(String, String)]) -> CredDigest {
    let mut sorted: Vec<&(String, String)> = pairs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = blake3::Hasher::new();
    for (k, v) in sorted {
        hasher.update(k.as_bytes());
        hasher.update(b"=");
        hasher.update(v.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().into()
}

/// Producer + consumer pair for a single credential digest. The
/// consumer is wrapped in `Arc` so concurrent calls can hold it
/// across awaits without owning it; the producer is `Clone` already
/// (FutureProducer is internally `Arc`-based).
pub struct KafkaClientBundle {
    pub producer: FutureProducer,
    pub consumer: Arc<StreamConsumer<rdkafka::consumer::DefaultConsumerContext>>,
}

struct ClientEntry {
    bundle: Arc<KafkaClientBundle>,
    cred_keys: Vec<(String, String)>,
    last_used: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct ClientRegistryConfig {
    pub max_entries: usize,
    pub idle_eviction: Duration,
}

impl Default for ClientRegistryConfig {
    fn default() -> Self {
        Self {
            max_entries: 256,
            idle_eviction: Duration::from_secs(15 * 60),
        }
    }
}

struct Inner {
    clients: HashMap<CredDigest, ClientEntry>,
}

/// Bounded per-credential (producer, consumer) cache. See module
/// docs.
pub struct ClientRegistry {
    inner: Arc<AsyncMutex<Inner>>,
    config: ClientRegistryConfig,
    epoch: Instant,
}

impl ClientRegistry {
    #[must_use]
    pub fn new(config: ClientRegistryConfig) -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(Inner {
                clients: HashMap::new(),
            })),
            config,
            epoch: Instant::now(),
        }
    }

    fn now_millis(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Look up an existing bundle or build a fresh one via `build`.
    /// The build closure runs at most once per cache miss; the
    /// outer mutex is dropped during the connect so unrelated
    /// digests don't serialise.
    pub async fn get_or_build<F, Fut>(
        &self,
        digest: CredDigest,
        cred_keys: Vec<(String, String)>,
        build: F,
    ) -> Result<Arc<KafkaClientBundle>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<KafkaClientBundle>>,
    {
        let guard = self.inner.lock().await;
        if let Some(entry) = guard.clients.get(&digest) {
            entry.last_used.store(self.now_millis(), Ordering::Relaxed);
            return Ok(Arc::clone(&entry.bundle));
        }
        drop(guard);
        let bundle = Arc::new(build().await?);
        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.clients.get(&digest) {
            entry.last_used.store(self.now_millis(), Ordering::Relaxed);
            return Ok(Arc::clone(&entry.bundle));
        }
        guard.clients.insert(
            digest,
            ClientEntry {
                bundle: Arc::clone(&bundle),
                cred_keys,
                last_used: AtomicU64::new(self.now_millis()),
            },
        );
        if guard.clients.len() > self.config.max_entries
            && let Some(oldest_digest) = guard
                .clients
                .iter()
                .min_by_key(|(_, e)| e.last_used.load(Ordering::Relaxed))
                .map(|(d, _)| *d)
        {
            guard.clients.remove(&oldest_digest);
            metrics::counter!(
                "mcpg_kafka_client_registry_evictions_total",
                "reason" => "lru",
            )
            .increment(1);
        }
        Ok(bundle)
    }

    /// Drop entries whose `cred_keys` contains the given pair.
    pub async fn evict_for(&self, plugin_id: &str, target: &str) -> usize {
        let mut guard = self.inner.lock().await;
        let to_drop: Vec<CredDigest> = guard
            .clients
            .iter()
            .filter(|(_, e)| {
                e.cred_keys
                    .iter()
                    .any(|(p, t)| p == plugin_id && t == target)
            })
            .map(|(d, _)| *d)
            .collect();
        let count = to_drop.len();
        for d in to_drop {
            guard.clients.remove(&d);
        }
        if count > 0 {
            metrics::counter!(
                "mcpg_kafka_client_registry_evictions_total",
                "reason" => "revoked",
            )
            .increment(count as u64);
        }
        count
    }

    /// Drop every entry. Called from the secret-rotation
    /// subscriber when a `vault://...` URI tied to this profile
    /// rotates. Mirrors the HTTP/SQL/NATS plugins' shape: per-
    /// profile monolithic eviction.
    pub async fn evict_for_secret(&self, _secret_ref: &str) -> usize {
        let mut guard = self.inner.lock().await;
        let count = guard.clients.len();
        guard.clients.clear();
        if count > 0 {
            metrics::counter!(
                "mcpg_kafka_client_registry_evictions_total",
                "reason" => "secret_rotation",
            )
            .increment(count as u64);
        }
        count
    }

    /// Drop entries whose `last_used` age exceeds
    /// `config.idle_eviction`.
    pub async fn sweep_idle(&self) -> usize {
        let mut guard = self.inner.lock().await;
        let now = self.now_millis();
        let threshold_ms = self.config.idle_eviction.as_millis() as u64;
        let to_drop: Vec<CredDigest> = guard
            .clients
            .iter()
            .filter(|(_, e)| {
                let last = e.last_used.load(Ordering::Relaxed);
                now.saturating_sub(last) > threshold_ms
            })
            .map(|(d, _)| *d)
            .collect();
        let count = to_drop.len();
        for d in to_drop {
            guard.clients.remove(&d);
        }
        if count > 0 {
            metrics::counter!(
                "mcpg_kafka_client_registry_evictions_total",
                "reason" => "idle",
            )
            .increment(count as u64);
        }
        count
    }

    pub async fn len(&self) -> usize {
        self.inner.lock().await.clients.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.clients.is_empty()
    }
}

/// Idle-bundle sweeper guard. Holding this Arc keeps the spawned
/// task alive; dropping the last clone cancels it.
pub struct IdleSweeper {
    _cancel_guard: DropGuard,
}

#[must_use]
pub fn spawn_idle_sweeper(
    backend_name: String,
    registry: Arc<ClientRegistry>,
    interval: Duration,
) -> Arc<IdleSweeper> {
    let token = CancellationToken::new();
    let guard = IdleSweeper {
        _cancel_guard: token.clone().drop_guard(),
    };
    tokio::spawn(idle_sweep_loop(backend_name, registry, interval, token));
    Arc::new(guard)
}

async fn idle_sweep_loop(
    backend_name: String,
    registry: Arc<ClientRegistry>,
    interval: Duration,
    cancel: CancellationToken,
) {
    info!(
        target: "mcpg::kafka::client_registry",
        backend = %backend_name,
        interval_ms = interval.as_millis() as u64,
        "kafka client idle sweeper: started"
    );
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                debug!(
                    target: "mcpg::kafka::client_registry",
                    backend = %backend_name,
                    "kafka client idle sweeper: cancelled"
                );
                return;
            }
            _ = ticker.tick() => {
                let evicted = registry.sweep_idle().await;
                if evicted > 0 {
                    info!(
                        target: "mcpg::kafka::client_registry",
                        backend = %backend_name,
                        evicted = evicted,
                        "evicted idle Kafka client bundles"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_order_independent() {
        let a = digest_credential_bundle(&[
            ("bootstrap_servers".into(), "broker:9092".into()),
            ("sasl_username".into(), "alice".into()),
        ]);
        let b = digest_credential_bundle(&[
            ("sasl_username".into(), "alice".into()),
            ("bootstrap_servers".into(), "broker:9092".into()),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn digest_distinguishes_inputs() {
        let a = digest_credential_bundle(&[("sasl_password".into(), "alpha".into())]);
        let b = digest_credential_bundle(&[("sasl_password".into(), "beta".into())]);
        assert_ne!(a, b);
    }

    #[test]
    fn static_digest_stable() {
        assert_eq!(static_digest(), static_digest());
    }
}
