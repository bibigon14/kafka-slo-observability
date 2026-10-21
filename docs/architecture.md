# Architecture

## Topology

```
                 Oracle Cloud Always Free Ampere A1 VM
                 (2 OCPU, 12 GB RAM, arm64, Ubuntu 24.04)
 +----------------------------------------------------------------------+
 |                        k3s v1.36 (single node)                       |
 |                                                                      |
 |   namespace: kafka                       namespace: monitoring       |
 |   +--------------------------+           +------------------------+  |
 |   | Strimzi Cluster Operator |           | kube-prometheus-stack  |  |
 |   | (watches Kafka CRs,      |           |   Prometheus Operator  |  |
 |   |  reconciles brokers)     |           |   Prometheus (TSDB)    |  |
 |   +--------------------------+           |   Grafana + sidecar    |  |
 |             | manages                    |   Alertmanager         |  |
 |             v                            +------------------------+  |
 |   +----------------------------------+              ^                |
 |   | KafkaNodePool "brokers" (KRaft)  |              | scrape         |
 |   |                                  |              | :9404          |
 |   |  demo-brokers-0 (controller+brkr)|<-------------+ /metrics       |
 |   |  demo-brokers-1 (controller+brkr)|   (PodMonitor)                |
 |   |  demo-brokers-2 (controller+brkr)|                               |
 |   |   - Kafka 3.9                    |                               |
 |   |   - JMX Prom Exporter on :9404   |                               |
 |   |   - 10Gi persistent-claim each   |                               |
 |   +----------------------------------+                               |
 |             ^                                                        |
 |             | 9092 plain / 9093 TLS                                  |
 |             |                                                        |
 |   +---------+------------+     +------------------------+            |
 |   | demo-producer        |     | demo-consumer          |            |
 |   | (console-producer)   |     | (console-consumer)     |            |
 |   | ~5 msg/s to          |     | group: demo-consumer-  |            |
 |   | demo-events          |     |        group           |            |
 |   +----------------------+     +------------------------+            |
 +----------------------------------------------------------------------+
```

## Metric sources - two layers, no overlap

The Kafka JMX exporter container that Strimzi injects into every broker
pod listens on `:9404` and already includes the JVM collector that
Prometheus Client ships with. That collector produces exactly the metric
names the upstream Strimzi Grafana dashboard queries:

- `jvm_memory_used_bytes{area="heap"|"nonheap"}`
- `jvm_memory_committed_bytes{area=...}`
- `jvm_gc_collection_seconds_count{gc=...}`
- `jvm_gc_collection_seconds_sum{gc=...}`
- `jvm_threads_current`, `jvm_threads_daemon`
- `jvm_classes_currently_loaded`
- `jvm_buffer_pool_*`

Our custom `kafka-metrics-config.yml` only adds Kafka MBean rules on top
of that. We deliberately do NOT add JVM rules there - duplicating the
built-in JVM collector crashes the exporter at scrape time with
`duplicate metric name`.

The Strimzi cluster operator pod exposes metrics via Micrometer at `:8080`
instead, with Micrometer naming (`jvm_memory_used_bytes` with `id` and
`area` labels but a different label set). A small PrometheusRule
(`manifests/monitoring/jvm-rename-rules.yaml`) bridges the two for
cross-pod comparisons when needed.

## PodMonitor relabeling

The upstream Strimzi dashboard JSON filters every panel on labels like
`strimzi_io_name="$strimzi_cluster_name-kafka"` and templates the pod
name from `strimzi_io_pod_name`. In Kubernetes pod metadata those live
as `strimzi.io/name` and `strimzi.io/pod-name`; the slash and dot don't
survive the metric-name grammar, so the PodMonitor must explicitly lift
them via `relabelings`:

```yaml
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_cluster]
  targetLabel: strimzi_io_cluster
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_name]
  targetLabel: strimzi_io_name
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_kind]
  targetLabel: strimzi_io_kind
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_broker_role]
  targetLabel: strimzi_io_broker_role
- sourceLabels: [__meta_kubernetes_pod_label_strimzi_io_pod_name]
  targetLabel: strimzi_io_pod_name
```

Without these the scrape target stays healthy and metrics flow, but
every dashboard panel reads "No data" because the dashboard's filter
never matches.

## SLO alert math

The two burn-rate alerts come straight from the Google SRE Workbook
multi-window pattern:

- **Fast / page**: 14.4x burn over 1h means the slow-rolling budget would
  be exhausted in `30d / 14.4 = ~2d` if the rate continued. That's an
  incident, not a drip. Short confirmation window (5m) keeps the alert
  from oscillating on a single bad scrape.
- **Slow / ticket**: 6x burn over 6h is a slower drain. It's not an
  incident, but it would eat the monthly budget in ~5 days if ignored.
  30m confirmation window because at this rate a 5-minute flap doesn't
  matter.

The 99.9% availability SLO leaves `30d * 0.1% = ~43min` of monthly
budget. Fast-burn fires if we spend 2% of that (~52s of unavailability)
in 5 minutes; slow-burn fires if we spend 10% (~4min 20s) over 30
minutes. Numbers come from the workbook's Table 5-5, not from feel.

## Resource budget

The whole kafka namespace fits well inside the OCI Always Free Ampere
A1 shape (2 OCPU / 12 GB RAM) alongside the other portfolio projects on
the same VM:

| Pod                        | CPU request | Memory request | Memory limit |
|---------------------------|------------:|---------------:|-------------:|
| demo-brokers-0            | 250m        | 1Gi            | 2Gi          |
| demo-brokers-1            | 250m        | 1Gi            | 2Gi          |
| demo-brokers-2            | 250m        | 1Gi            | 2Gi          |
| strimzi-cluster-operator  | 200m        | 384Mi          | 384Mi        |
| demo-producer             | 50m         | 128Mi          | 256Mi        |
| demo-consumer             | 50m         | 128Mi          | 256Mi        |
| **Kafka subtotal**        | **1050m**   | **3.4 GiB**    | **7.1 GiB**  |

The brokers initially ran at 1Gi memory limit and were OOMKilled under
2 MB/s producer throughput once the demo workload came online (JVM
needed room for the direct byte buffers plus pagecache ingest).
Bumping to 2Gi + explicit `-Xms 1024m -Xmx 1536m` settled them.

## Known gaps

- No TLS between producer/consumer and brokers in v0.1 - internal
  listener on `plain:9092`. The `tls:9093` listener is enabled on the
  broker but not consumed by the demo clients.
- Grafana dashboard is the upstream Strimzi JSON, not a custom one. A
  v0.2 custom dashboard would show the SLO burn-rate alerts side by
  side with the consumer lag timeseries.
- Alertmanager not wired to a notification channel in this repo. The
  upstream kube-prometheus-stack ships with a null receiver by default;
  routing to Telegram follows the pattern used in
  [`homelab-observability`](https://github.com/bibigon14/homelab-observability).
