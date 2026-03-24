#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SOCKET="${HOME}/.docker/run/docker.sock"
DOCKER_SOCKET="${DOCKER_SOCKET:-${DEFAULT_SOCKET}}"
COMMAND="${1:-}"

if [[ ! -S "${DOCKER_SOCKET}" && -S "/var/run/docker.sock" ]]; then
  DOCKER_SOCKET="/var/run/docker.sock"
fi

if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.env"
  set +a
fi

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-flink-dynamic-kafka-sink}"
APP_CLUSTER_ID="${APP_CLUSTER_ID:-default-cluster}"
APP_TOPIC="${APP_TOPIC:-dynamic-kafka-sink}"
APP_STREAM_PATTERN="${APP_STREAM_PATTERN:-^${APP_TOPIC}$}"
APP_RECORDS="${APP_RECORDS:-alpha,beta,gamma}"

usage() {
  cat <<'EOF'
Usage:
  ./run.sh setup   Install local prerequisites and prepare Kafka topic
  ./run.sh run     Prepare Kafka, build jar locally, and run the application
  ./run.sh down    Stop and remove application containers
EOF
}

require_docker() {
  docker info >/dev/null
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

ensure_env_file() {
  if [[ ! -f "${SCRIPT_DIR}/.env" ]]; then
    echo "Missing .env file at project root." >&2
    return 1
  fi
}

wait_for_kafka() {
  echo "Waiting for Kafka broker to become ready..."
  for _ in {1..30}; do
    if docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka \
      kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Kafka broker did not become ready in time." >&2
  return 1
}

setup_env() {
  ensure_java_17
  ensure_env_file
  require_docker
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d kafka
  wait_for_kafka
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka \
    kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "${APP_TOPIC}" --partitions 1 --replication-factor 1
}

run_application() {
  setup_env
  mvn -DskipTests -pl dynamic-kafka-sink-app -am package
  java -jar "${SCRIPT_DIR}/dynamic-kafka-sink-app/target/dynamic-kafka-sink-app-1.0-SNAPSHOT.jar" \
    --bootstrap-servers kafka:9092 \
    --stream-pattern "${APP_STREAM_PATTERN}" \
    --cluster-id "${APP_CLUSTER_ID}" \
    --records "${APP_RECORDS}"

  echo
  echo "Produced records:"
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka \
    kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic "${APP_TOPIC}" --from-beginning \
    --max-messages "$(awk -F',' '{print NF}' <<<"${APP_RECORDS}")"
}

case "${COMMAND}" in
  setup)
    setup_env
    ;;
  run)
    run_application
    ;;
  down)
    require_docker
    docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down -v
    ;;
  *)
    usage
    exit 1
    ;;
esac
