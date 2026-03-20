#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
DEFAULT_SOCKET="${HOME}/.docker/run/docker.sock"
DOCKER_SOCKET="${DOCKER_SOCKET:-${DEFAULT_SOCKET}}"
COMMAND="${1:-}"
BUILD_DIR="${SCRIPT_DIR}/.build"
IMAGE_FINGERPRINT_FILE="${BUILD_DIR}/image.fingerprint"
BUILDX_CACHE_DIR="${BUILD_DIR}/buildx-cache"
BUILDX_CACHE_DIR_NEW="${BUILD_DIR}/buildx-cache-new"

if [[ ! -S "${DOCKER_SOCKET}" && -S "/var/run/docker.sock" ]]; then
  DOCKER_SOCKET="/var/run/docker.sock"
fi

if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.env"
  set +a
fi

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-dynamic-kafka-sink-demo}"
DEMO_CLUSTER_ID="${DEMO_CLUSTER_ID:-demo-cluster}"
DEMO_TOPIC="${DEMO_TOPIC:-dynamic-kafka-sink-demo}"
DEMO_STREAM_PATTERN="${DEMO_STREAM_PATTERN:-^${DEMO_TOPIC}$}"
DEMO_RECORDS="${DEMO_RECORDS:-alpha,beta,gamma}"
DEMO_IMAGE="${DEMO_IMAGE:-dynamic-kafka-sink-demo:local}"

usage() {
  cat <<'EOF'
Usage:
  ./run.sh setup   Install local prerequisites and prepare Kafka topic
  ./run.sh run     Prepare Kafka, refresh the local image if needed, and run the demo
  ./run.sh push    Build or refresh the local demo image
  ./run.sh down    Stop and remove demo containers
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
  if [[ ! -f "${SCRIPT_DIR}/.env" && -f "${SCRIPT_DIR}/.env.example" ]]; then
    cp "${SCRIPT_DIR}/.env.example" "${SCRIPT_DIR}/.env"
  fi
}

compute_image_fingerprint() {
  python3 - "${REPO_ROOT}" <<'PY'
import hashlib
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1])
paths = [
    repo_root / "pom.xml",
    repo_root / "Dockerfile",
    repo_root / "src",
]

digest = hashlib.sha256()
for path in paths:
    if path.is_file():
        digest.update(str(path.relative_to(repo_root)).encode())
        digest.update(path.read_bytes())
    elif path.is_dir():
        for file_path in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(file_path.relative_to(repo_root)).encode())
            digest.update(file_path.read_bytes())

print(digest.hexdigest())
PY
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
    --topic "${DEMO_TOPIC}" --partitions 1 --replication-factor 1
}

build_image() {
  ensure_env_file
  require_docker
  mkdir -p "${BUILD_DIR}"
  local current_fingerprint
  current_fingerprint="$(compute_image_fingerprint)"

  if [[ -f "${IMAGE_FINGERPRINT_FILE}" ]] \
    && [[ "$(cat "${IMAGE_FINGERPRINT_FILE}")" == "${current_fingerprint}" ]] \
    && docker image inspect "${DEMO_IMAGE}" >/dev/null 2>&1; then
    echo "No relevant source changes detected. Reusing local image '${DEMO_IMAGE}'."
    return 0
  fi

  rm -rf "${BUILDX_CACHE_DIR_NEW}"
  if [[ -d "${BUILDX_CACHE_DIR}" ]]; then
    docker buildx build \
      --load \
      --cache-from "type=local,src=${BUILDX_CACHE_DIR}" \
      --cache-to "type=local,dest=${BUILDX_CACHE_DIR_NEW},mode=max" \
      -t "${DEMO_IMAGE}" \
      -f "${SCRIPT_DIR}/Dockerfile" \
      "${REPO_ROOT}"
  else
    docker buildx build \
      --load \
      --cache-to "type=local,dest=${BUILDX_CACHE_DIR_NEW},mode=max" \
      -t "${DEMO_IMAGE}" \
      -f "${SCRIPT_DIR}/Dockerfile" \
      "${REPO_ROOT}"
  fi

  rm -rf "${BUILDX_CACHE_DIR}"
  mv "${BUILDX_CACHE_DIR_NEW}" "${BUILDX_CACHE_DIR}"
  printf '%s\n' "${current_fingerprint}" > "${IMAGE_FINGERPRINT_FILE}"
}

run_demo() {
  setup_env
  build_image
  docker run --rm \
    --network "${COMPOSE_PROJECT_NAME}_default" \
    "${DEMO_IMAGE}" \
    --bootstrap-servers kafka:9092 \
    --stream-pattern "${DEMO_STREAM_PATTERN}" \
    --cluster-id "${DEMO_CLUSTER_ID}" \
    --records "${DEMO_RECORDS}"

  echo
  echo "Produced records:"
  docker compose -f "${SCRIPT_DIR}/docker-compose.yml" exec -T kafka \
    kafka-console-consumer --bootstrap-server kafka:9092 \
    --topic "${DEMO_TOPIC}" --from-beginning \
    --max-messages "$(awk -F',' '{print NF}' <<<"${DEMO_RECORDS}")"
}

case "${COMMAND}" in
  setup)
    setup_env
    ;;
  push)
    build_image
    ;;
  run)
    run_demo
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
