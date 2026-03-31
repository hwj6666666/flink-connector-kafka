#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config.yaml"
COMMAND="${1:-}"
MODE="${2:-}"

usage() {
  cat <<'EOF'
Usage:
  ./run.sh setup                 Verify local Kafka and create topics
  ./run.sh run dynamic           Run dynamic sink benchmark job
  ./run.sh run regular           Run regular KafkaSink benchmark job
EOF
}

yaml_get() {
  local key="$1"
  ruby -ryaml -e '
    data = YAML.load_file(ARGV[0]) || {}
    value = ARGV[1].split(".").reduce(data) { |memo, part| memo.is_a?(Hash) ? memo[part] : nil }
    puts(value.nil? ? "" : value)
  ' "${CONFIG_FILE}" "${key}"
}

ensure_config_file() {
  if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "Missing config file at ${CONFIG_FILE}." >&2
    return 1
  fi
}

load_config() {
  ensure_config_file

  DYNAMIC_CLUSTER_ID="$(yaml_get "dynamic.cluster_id")"
  DYNAMIC_TOPIC="$(yaml_get "dynamic.topic")"
  DYNAMIC_STREAM_PATTERN="$(yaml_get "dynamic.stream_pattern")"
  DYNAMIC_BOOTSTRAP_SERVERS="$(yaml_get "dynamic.bootstrap_servers")"
  DYNAMIC_EMIT_INTERVAL_MS="$(yaml_get "dynamic.emit_interval_ms")"
  DYNAMIC_PARALLELISM="$(yaml_get "dynamic.parallelism")"
  DYNAMIC_DISCOVERY_INTERVAL_MS="$(yaml_get "dynamic.discovery_interval_ms")"

  REGULAR_TOPIC="$(yaml_get "regular.topic")"
  REGULAR_BOOTSTRAP_SERVERS="$(yaml_get "regular.bootstrap_servers")"
  REGULAR_EMIT_INTERVAL_MS="$(yaml_get "regular.emit_interval_ms")"
  REGULAR_PARALLELISM="$(yaml_get "regular.parallelism")"

  DYNAMIC_CLUSTER_ID="${DYNAMIC_CLUSTER_ID:-default-cluster}"
  DYNAMIC_TOPIC="${DYNAMIC_TOPIC:-dynamic-kafka-sink}"
  DYNAMIC_STREAM_PATTERN="${DYNAMIC_STREAM_PATTERN:-^${DYNAMIC_TOPIC}$}"
  DYNAMIC_BOOTSTRAP_SERVERS="${DYNAMIC_BOOTSTRAP_SERVERS:-localhost:9092}"
  DYNAMIC_EMIT_INTERVAL_MS="${DYNAMIC_EMIT_INTERVAL_MS:-0}"
  DYNAMIC_PARALLELISM="${DYNAMIC_PARALLELISM:-1}"
  DYNAMIC_DISCOVERY_INTERVAL_MS="${DYNAMIC_DISCOVERY_INTERVAL_MS:-2000}"

  REGULAR_TOPIC="${REGULAR_TOPIC:-${DYNAMIC_TOPIC}}"
  REGULAR_BOOTSTRAP_SERVERS="${REGULAR_BOOTSTRAP_SERVERS:-${DYNAMIC_BOOTSTRAP_SERVERS}}"
  REGULAR_EMIT_INTERVAL_MS="${REGULAR_EMIT_INTERVAL_MS:-0}"
  REGULAR_PARALLELISM="${REGULAR_PARALLELISM:-1}"
}

java_major_version() {
  local version_output
  version_output="$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')"
  if [[ "${version_output}" == 1.* ]]; then
    awk -F'.' '{print $2}' <<<"${version_output}"
  else
    awk -F'.' '{print $1}' <<<"${version_output}"
  fi
}

ensure_java_17() {
  if command -v java >/dev/null 2>&1; then
    local current_major
    current_major="$(java_major_version || true)"
    if [[ -n "${current_major}" ]] && [[ "${current_major}" -ge 17 ]]; then
      return 0
    fi
  fi

  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Java 17+ is required. Please install JDK 17 manually for your OS." >&2
    return 1
  fi

  if ! command -v brew >/dev/null 2>&1; then
    echo "Homebrew is required for automatic JDK installation." >&2
    echo "Install Homebrew first: https://brew.sh/" >&2
    return 1
  fi

  echo "Installing JDK 17 (temurin@17) via Homebrew..."
  brew list --cask temurin@17 >/dev/null 2>&1 || brew install --cask temurin@17

  local detected_java_home
  detected_java_home="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  if [[ -z "${detected_java_home}" ]]; then
    echo "JDK 17 was installed but JAVA_HOME could not be detected." >&2
    return 1
  fi

  export JAVA_HOME="${detected_java_home}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
}

kafka_topics_cmd() {
  if command -v kafka-topics >/dev/null 2>&1; then
    echo "kafka-topics"
    return 0
  fi
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    echo "kafka-topics.sh"
    return 0
  fi
  echo "Kafka CLI not found. Install Kafka via Homebrew and ensure kafka-topics is in PATH." >&2
  return 1
}

ensure_local_kafka_ready() {
  local topics_cmd
  topics_cmd="$(kafka_topics_cmd)"
  echo "Checking local Kafka broker at ${DYNAMIC_BOOTSTRAP_SERVERS} ..."
  for _ in {1..30}; do
    if "${topics_cmd}" --bootstrap-server "${DYNAMIC_BOOTSTRAP_SERVERS}" --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Local Kafka broker is not reachable at ${DYNAMIC_BOOTSTRAP_SERVERS}." >&2
  return 1
}

setup_local_env() {
  ensure_java_17
  load_config
  ensure_local_kafka_ready
  local topics_cmd
  topics_cmd="$(kafka_topics_cmd)"
  "${topics_cmd}" --bootstrap-server "${DYNAMIC_BOOTSTRAP_SERVERS}" --create --if-not-exists \
    --topic "${DYNAMIC_TOPIC}" --partitions 1 --replication-factor 1
  "${topics_cmd}" --bootstrap-server "${REGULAR_BOOTSTRAP_SERVERS}" --create --if-not-exists \
    --topic "${REGULAR_TOPIC}" --partitions 1 --replication-factor 1
}

build_app() {
  setup_local_env
  mvn -DskipTests -pl app -am package
}

run_dynamic_job() {
  build_app
  java -cp "${SCRIPT_DIR}/app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar" \
    org.apache.flink.dynamic.sink.job.DynamicJob \
    --bootstrap-servers "${DYNAMIC_BOOTSTRAP_SERVERS}" \
    --stream-pattern "${DYNAMIC_STREAM_PATTERN}" \
    --cluster-id "${DYNAMIC_CLUSTER_ID}" \
    --emit-interval-ms "${DYNAMIC_EMIT_INTERVAL_MS}" \
    --parallelism "${DYNAMIC_PARALLELISM}" \
    --discovery-interval-ms "${DYNAMIC_DISCOVERY_INTERVAL_MS}" \
    --config-file "${CONFIG_FILE}"
}

run_regular_job() {
  build_app
  java -cp "${SCRIPT_DIR}/app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar" \
    org.apache.flink.dynamic.sink.job.RegularJob \
    --bootstrap-servers "${REGULAR_BOOTSTRAP_SERVERS}" \
    --topic "${REGULAR_TOPIC}" \
    --emit-interval-ms "${REGULAR_EMIT_INTERVAL_MS}" \
    --parallelism "${REGULAR_PARALLELISM}"
}

case "${COMMAND}" in
  setup)
    setup_local_env
    ;;
  run)
    case "${MODE}" in
      dynamic)
        run_dynamic_job
        ;;
      regular)
        run_regular_job
        ;;
      *)
        usage
        exit 1
        ;;
    esac
    ;;
  *)
    usage
    exit 1
    ;;
esac
