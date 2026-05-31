#!/usr/bin/env bash
# 论文辅助脚本：
#   ./run.sh pdf    清除旧辅助文件并完整重编译论文
#   ./run.sh word   统计论文字数
#   ./run.sh clean  清理中间文件
#   ./run.sh view   打开生成的 PDF
#   ./run.sh pvc    持续监听并自动重编译

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMMAND="${1:-}"

usage() {
  cat <<'EOF'
Usage:
  ./run.sh pdf    Clean and rebuild thesis PDF
  ./run.sh word   Count thesis words/chars
  ./run.sh clean  Remove LaTeX intermediate files
  ./run.sh view   Open generated PDF
  ./run.sh pvc    Watch files and rebuild automatically

Commands:
  pdf    先执行 cleanall，再完整编译论文并按“论文题目_时间戳”生成 PDF
  word   调用 texcount 统计中文字数/总字数
  clean  清理论文编译产生的辅助文件
  view   打开当前生成的论文 PDF
  pvc    进入 latexmk 持续监听模式，源文件变更后自动重编译
EOF
}

get_thesis_title() {
  python3 - <<'PY'
import pathlib
import re

setup_path = pathlib.Path(r"/Users/wenjiehu/graduate/flink-connector-kafka/docs/SJTUThesis/setup.tex")
text = setup_path.read_text(encoding="utf-8")
match = re.search(r"zh\s*/\s*title\s*=\s*\{([^}]*)\}", text)
title = match.group(1).strip() if match else "thesis"
title = re.sub(r"[\\/:*?\"<>|]", "_", title)
title = re.sub(r"\s+", "", title)
print(title or "thesis")
PY
}

run_pdf() {
  local thesis_title
  local timestamp
  local output_pdf
  echo ">>> ${SCRIPT_DIR}"
  echo ">>> make cleanall all"
  cd "${SCRIPT_DIR}"
  make cleanall all
  thesis_title="$(get_thesis_title)"
  timestamp="$(date +"%Y%m%d_%H%M%S")"
  output_pdf="${DOCS_DIR}/${thesis_title}_${timestamp}.pdf"
  mv -f "${DOCS_DIR}/main.pdf" "${output_pdf}"
  echo ">>> 完成: ${output_pdf}"
}

run_wordcount() {
  echo ">>> ${SCRIPT_DIR}"
  echo ">>> make wordcount"
  cd "${SCRIPT_DIR}"
  make wordcount
}

run_clean() {
  echo ">>> ${SCRIPT_DIR}"
  echo ">>> make cleanall"
  cd "${SCRIPT_DIR}"
  make cleanall
}

run_view() {
  echo ">>> ${SCRIPT_DIR}"
  echo ">>> make view"
  cd "${SCRIPT_DIR}"
  make view
}

run_pvc() {
  echo ">>> ${SCRIPT_DIR}"
  echo ">>> make pvc"
  cd "${SCRIPT_DIR}"
  make pvc
}

case "${COMMAND}" in
  "")
    usage
    ;;
  pdf)
    run_pdf
    ;;
  word)
    run_wordcount
    ;;
  clean)
    run_clean
    ;;
  view)
    run_view
    ;;
  pvc)
    run_pvc
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: ${COMMAND}" >&2
    usage
    exit 1
    ;;
esac
