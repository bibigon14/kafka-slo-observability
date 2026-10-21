#!/usr/bin/env bash
# burn-rate-demo.sh
#
# Trigger the KafkaConsumerLagBurnRateFast alert end-to-end: scale the
# consumer to zero so the producer piles up messages, wait for the
# recording rule to accumulate 5 minutes of "lag over SLO threshold",
# and let the burn-rate alert fire into Telegram.
#
# Prereqs:
#   - kafka cluster deployed from manifests/kafka/
#   - monitoring stack deployed from manifests/monitoring/
#   - producer and consumer deployed from manifests/workload/
#   - AlertmanagerConfig wired to a notification channel
#     (manifests/monitoring/kafka-alertmanager-config.yaml)
#
# Safety:
#   - Trap on EXIT scales the consumer back and restores retention,
#     so a Ctrl+C halfway through still leaves the cluster in a
#     reasonable state.
#   - Script does NOT ramp producer rate - the default demo-producer
#     at ~2 MB/s is enough to burn the error budget inside two minutes.

set -euo pipefail

NAMESPACE=${NAMESPACE:-kafka}
TOPIC=${TOPIC:-demo-events}
RETAIN_MS_NORMAL=${RETAIN_MS_NORMAL:-3600000}   # 1h
RETAIN_MS_DEMO=${RETAIN_MS_DEMO:-900000}        # 15m, big enough for the demo
SEG_BYTES_DEMO=${SEG_BYTES_DEMO:-104857600}     # 100MB

say() { printf '\n>>> %s\n' "$*"; }

restore() {
  say "Restoring cluster to normal state"
  kubectl -n "${NAMESPACE}" scale deployment demo-consumer --replicas=1 || true
  kubectl -n "${NAMESPACE}" exec demo-brokers-0 -c kafka -- bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name "${TOPIC}" \
    --alter --add-config "retention.ms=${RETAIN_MS_NORMAL}" || true
  say "Done. Consumer should drain the lag within a minute."
}
trap restore EXIT INT TERM

say "Setting retention to ${RETAIN_MS_DEMO}ms so the lag can accumulate"
kubectl -n "${NAMESPACE}" exec demo-brokers-0 -c kafka -- bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name "${TOPIC}" \
  --alter --add-config "retention.ms=${RETAIN_MS_DEMO},segment.bytes=${SEG_BYTES_DEMO},segment.ms=${RETAIN_MS_DEMO}"

say "Scaling consumer to 0 - producer will keep writing, lag will climb"
kubectl -n "${NAMESPACE}" scale deployment demo-consumer --replicas=0

say "Waiting for the burn-rate alert to accumulate (5-7 minutes)."
say "Watch 'kubectl get prometheusrule kafka-slo -n kafka' or your Alertmanager UI."
say "You can safely Ctrl+C once the alert fires - the trap will clean up."
sleep 420   # 7 minutes

say "Elapsed. KafkaConsumerLagBurnRateFast should be firing now."
say "Press Ctrl+C or wait for this script to exit to restore the consumer."
sleep 60
