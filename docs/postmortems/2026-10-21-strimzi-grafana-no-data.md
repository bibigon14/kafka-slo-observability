# Postmortem: Strimzi Grafana dashboard reads "No data" even though 92 jvm_* lines are scraped per broker

**Date**: 2026-10-21
**Author**: Dmitry Stepanov
**Status**: Resolved
**Impact**: Dashboard panels for broker JVM internals showed "No data" for the duration of first roll-out (~90 minutes). No production impact - this is a portfolio project - but a real-world version of this bug would mask JVM pressure during an incident.

## Summary

On the first roll-out of `kafka-slo-observability` on OCI Ampere A1, the upstream Strimzi Grafana dashboard imported cleanly, the Prometheus PodMonitor scrape targets came up healthy, and 92 `jvm_*` lines per broker were visible when scraping the exporter port directly. Yet all four JVM panels on the dashboard (`JVM Memory Used`, `JVM GC Time`, `JVM GC Count`, `JVM Thread Count`) read "No data". The failure mode gave contradictory signals - the data was being produced, the data was being scraped, Prometheus held the series, and the dashboard still refused to render.

Root cause was a single missing relabel rule in the PodMonitor: the upstream dashboard filters every JVM panel on `strimzi_io_name="<cluster>-kafka"`, a label that lives on the pod as `strimzi.io/name=demo-kafka` but is not lifted onto the metric labels by default. Without that explicit relabel, every JVM panel filter missed, and Grafana rendered "No data" over healthy metrics.

A second, independent trap was uncovered during the same incident: attempting to re-expose JVM metrics by adding JVM collector rules to the Kafka JMX Exporter's rules config file crashed the exporter at scrape time with `duplicate metric name`, because the exporter already ships its own JVM collector.

## Timeline (America/Los_Angeles)

- **22:55** - First roll-out complete. Dashboard shows 3 brokers online, 159 online replicas, 0 under-replicated, 2 MB/s produce, 2K msg/s. Kafka-side panels all working. JVM panel block reads "No data".
- **22:56** - User flags the JVM gap. First hypothesis: recording rule `jvm-rename-rules.yaml` not applied.
- **22:59** - `kubectl get prometheusrule` confirms the rule is present and `jvm_memory_bytes_used` returns 14 series. Hypothesis invalidated.
- **23:03** - Noticed that all 14 series come from `strimzi-cluster-operator` pod on job `kafka/kafka-entity-operator-metrics`. Zero series from broker pods. New hypothesis: broker JMX exporter is not exposing JVM metrics at all.
- **23:05** - `kubectl exec demo-brokers-0 -- curl :9404/metrics | grep jvm_` returns 92 lines, including `jvm_memory_used_bytes{area="heap"}` and `jvm_gc_collection_seconds_count{gc=...}` - the exact metric names the dashboard queries. Hypothesis invalidated.
- **23:06** - New hypothesis: PodMonitor scrape target broken. `curl /api/v1/targets` shows target healthy, `scrapeUrl: http://10.42.0.141:9404/metrics`, no last error, scraping on a 30s interval.
- **23:07** - PromQL `jvm_memory_committed_bytes{pod="demo-brokers-0"}` returns 2 series with full labels including `strimzi_io_cluster=demo` and `area=heap`. Metrics ARE scraped, ARE stored, ARE queryable. The dashboard still shows "No data".
- **23:12** - Opened the dashboard JSON directly from the ConfigMap. First JVM panel query: `sum(jvm_memory_used_bytes{namespace="$kubernetes_namespace",kubernetes_pod_name=~"$strimzi_cluster_name-$kafka_broker",strimzi_io_name="$strimzi_cluster_name-kafka"}) by (kubernetes_pod_name)`. The filter requires `strimzi_io_name`, which the broker series does not carry.
- **23:14** - PodMonitor relabel extended with `strimzi.io/name` and `strimzi.io/pod-name` liftings. Prometheus Operator reconciles ~15s later.
- **23:16** - Dashboard refresh: all four JVM panels fill in. Confirmed.
- **23:22** - Second incident starts: host disk falls from 86% to 94% as `demo-producer` sends 4 MB/s to a 3-replica topic (12 MB/s host write) with 1h retention and 1GB segments on a k3s local-path PVC that does not enforce size. Producer scaled to 0, retention and segment size shrunk, host disk recovered to 74%.

## Hypotheses tried and ruled out

| # | Hypothesis | Signal that killed it |
|---|---|---|
| 1 | `jvm-rename-rules.yaml` was never applied | `kubectl get prometheusrule` showed it present and the rule's output had 14 series |
| 2 | The recording rule's `label_replace` regex on `id` label doesn't match broker-side labels | Direct source metrics on broker had the same `id` values (`G1 Eden Space`, etc.) |
| 3 | Broker JMX exporter is not exposing JVM at all | `curl :9404/metrics` on the pod returned 92 `jvm_*` lines including the exact names the dashboard queries |
| 4 | PodMonitor scrape target broken | `/api/v1/targets` showed health=up, no lastError, correct scrape URL |
| 5 | A `metric_relabel_configs` was dropping `jvm_*` from this job | PodMonitor has none; `count(jvm_memory_committed_bytes) by (job)` showed 6 series on the broker job |
| 6 | Prometheus cache stale after relabel change | Already waited 60s between apply and re-query. Not the issue - the series were missing, not stale |

Each one was cheap to test but added five to ten minutes to the resolution window. The missing step was reading the dashboard panel query *before* chasing storage and scraping, because the panel query is what defines the contract between the storage and the UI.

## Root cause

The upstream Strimzi Kafka dashboard filters every JVM panel on `strimzi_io_name="$strimzi_cluster_name-kafka"` (value: `demo-kafka`). This label does not exist on the metric series by default. On the pod it lives as `strimzi.io/name` (dot and slash), and Prometheus Operator's PodMonitor exposes it to relabel rules as `__meta_kubernetes_pod_label_strimzi_io_name`, but will NOT lift it onto the final metric labels unless there is an explicit `sourceLabels/targetLabel` rule.

The default PodMonitor relabel block lifted `strimzi_io_cluster`, `strimzi_io_kind` and `strimzi_io_broker_role` but not `strimzi_io_name` or `strimzi_io_pod_name` (the second one also used by dashboard template variables). The result:

- Kafka-side panels: happy, because they filter on `strimzi_io_cluster` and `strimzi_io_broker_role` which were present.
- JVM panels: silently empty, because `strimzi_io_name` was missing from the series.

Fix: four lines in the PodMonitor.

```yaml
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_name]
  targetLabel: strimzi_io_name
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_pod_name]
  targetLabel: strimzi_io_pod_name
```

## Secondary trap: duplicate JVM metric name

During the earlier roll-out an attempt was made to re-expose JVM metrics by adding JVM collector rules to `kafka-metrics-config.yml` (the ConfigMap mounted into the Kafka JMX Exporter). That crashed the exporter at scrape time with:

```
java.lang.IllegalStateException: jvm_classes_loaded: duplicate metric name
```

Kafka 3.9's JMX Exporter already includes the JVM collector internally. Adding JVM MBean rules on top caused the two sources to register the same metric name twice, which the Prometheus Client library rejects. The exporter crashed on the first scrape after reload, Prometheus turned the target red, and the kafka panels went dark too.

The fix was to NOT add JVM rules to `kafka-metrics-config.yml` and let the exporter's built-in collector handle them. A separate `PrometheusRule` (`jvm-rename-rules.yaml`) was added for the different problem of bridging the entity operator pod's Micrometer naming (`jvm_memory_used_bytes` with Micrometer labels) to the Prometheus Client naming the dashboard expects.

## What we're changing

1. **PodMonitor relabel is now exhaustive**: the committed `manifests/monitoring/kafka-podmonitor.yaml` lifts every `strimzi.io/*` label the upstream dashboards reference. Future dashboards that reference other labels will fail the same way, but the fix pattern is now in the repo.
2. **`docs/architecture.md` documents the two metric sources explicitly**: broker JMX Exporter at `:9404` with built-in JVM collector (Prometheus Client naming), vs entity operator Micrometer at `:8080` (Micrometer naming). The rule for not adding JVM rules on top of the exporter is spelled out.
3. **README includes this postmortem** so the next person reading the project sees the trap before they open the dashboard.

## What we are NOT changing

- Not switching away from the upstream Strimzi dashboard. The custom-dashboard-with-SLO-burn-rate panels work is a v0.2 item, not a reaction to this bug.
- Not enforcing PVC quotas - the broker disk-fill incident during the same session is covered in its own note (`docs/architecture.md` resource budget section). k3s local-path provisioner does not enforce quotas by design; the real production remediation is a storage class that does.

## What would catch this faster next time

- **Read the dashboard panel query before touching the scrape pipeline.** The panel filter is the contract between stored series and rendered values. A missing label in that filter is zero cost to spot and up to an hour of debugging if ignored.
- **A smoke test on the PodMonitor**: for each panel query, assert at least one series is returned. This belongs in CI, not in a human eye at 11pm.
- **Alert on dashboard-panel freshness**: a PrometheusRule that computes the panel query (or close to it) and fires when it returns an empty vector would have paged in minutes, not been noticed by a human staring at the UI.
