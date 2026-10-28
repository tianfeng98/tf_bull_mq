#!/usr/bin/env bash
set -euo pipefail

# 发布脚本：先跑静态分析和测试，再做 pub.dev dry-run；
# 加 --publish 才真正上传。--publisher 可选，用于在 dry-run / publish
# 前校验 pub.dev 上的 package owner，防止发布到错误 publisher 名下。
#
# tf_bull_mq 是纯 Dart 包，脚本统一使用 fvm dart，
# 由本仓库的 .fvm/ 锁定 SDK 版本。

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

OFFICIAL_PUB_HOSTED_URL="https://pub.dev"
DO_PUBLISH="false"
EXPECTED_PUBLISHER="tf98.work"

usage() {
  cat <<'EOF'
Usage:
  .scripts/publish_pubdev.sh                                # Run pre-publish checks + dry-run
  .scripts/publish_pubdev.sh --publish                      # Run checks then publish
  .scripts/publish_pubdev.sh --publisher tf98.work          # Verify publisher before dry-run/publish
  .scripts/publish_pubdev.sh --publish --publisher tf98.work

Steps:
  1) fvm dart pub get
  2) fvm dart analyze
  3) fvm dart test
  4) PUB_HOSTED_URL=https://pub.dev fvm dart pub publish --dry-run
  5) (optional) PUB_HOSTED_URL=https://pub.dev fvm dart pub publish -f

Notes:
  - pub publish does not support a "--publisher" flag.
  - This script's --publisher option is a safety check against pub.dev ownership.
  - Working directory is the repository root, as tf_bull_mq is a standalone package.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish)
      DO_PUBLISH="true"
      shift
      ;;
    --publisher)
      if [[ -z "${2:-}" ]]; then
        echo "Missing value for --publisher"
        usage
        exit 2
      fi
      EXPECTED_PUBLISHER="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 2
      ;;
  esac
done

if [[ -n "$EXPECTED_PUBLISHER" ]]; then
  PACKAGE_NAME="$(awk -F': *' '/^name:/{print $2; exit}' pubspec.yaml | tr -d '"' | tr -d "'")"
  if [[ -z "$PACKAGE_NAME" ]]; then
    echo "Unable to read package name from pubspec.yaml"
    exit 1
  fi

  echo "[CHECK] Verifying publisher for package '$PACKAGE_NAME'"
  # 注意：不能用 -f，否则 HTTP 404 会让 curl 返回非零退出码；
  # 我们要区分 200 / 404 / 网络错误，靠 HTTP_CODE 而不是靠退出码。
  HTTP_CODE="$(curl -sS -o /tmp/pub_publisher_${PACKAGE_NAME}.json \
    -w '%{http_code}' \
    "${OFFICIAL_PUB_HOSTED_URL}/api/packages/${PACKAGE_NAME}/publisher" 2>/dev/null)"
  if [[ -z "$HTTP_CODE" ]]; then
    HTTP_CODE="000"  # 网络不可达或 curl 本身失败
  fi
  ACTUAL_PUBLISHER="$(sed -n 's/.*"publisherId":"\([^"]*\)".*/\1/p' \
    "/tmp/pub_publisher_${PACKAGE_NAME}.json" 2>/dev/null || true)"

  if [[ "$HTTP_CODE" == "404" ]]; then
    # 包还未发布过，首次 publish 由发布者账号直接 ownership，
    # 待包归属稳定后再在 pub.dev 后台转移到 verified publisher。
    echo "[CHECK] Package '$PACKAGE_NAME' not found on pub.dev (HTTP 404)."
    echo "         First publish will associate ownership with your account."
    echo "         After publish, transfer it to verified publisher '$EXPECTED_PUBLISHER'."
  elif [[ "$HTTP_CODE" != "200" ]]; then
    echo "[WARN] Unable to query pub.dev publisher API (HTTP $HTTP_CODE); skipping verification."
  elif [[ -z "$ACTUAL_PUBLISHER" ]]; then
    # 包已存在但尚未指定 verified publisher，允许继续。
    echo "[CHECK] Package '$PACKAGE_NAME' has no verified publisher yet."
    echo "         After publish, transfer it to verified publisher '$EXPECTED_PUBLISHER'."
  elif [[ "$ACTUAL_PUBLISHER" != "$EXPECTED_PUBLISHER" ]]; then
    # 包已在错误 publisher 名下，拒绝发布防止所有权错乱。
    echo "Publisher mismatch: expected '$EXPECTED_PUBLISHER', actual '$ACTUAL_PUBLISHER'."
    echo "Aborting publish workflow to prevent publishing under the wrong owner."
    exit 1
  else
    echo "[CHECK] Publisher verified: $ACTUAL_PUBLISHER"
  fi
fi

echo "[1/4] fvm dart pub get"
fvm dart pub get

echo "[2/4] fvm dart analyze"
fvm dart analyze

echo "[3/4] fvm dart test"
fvm dart test

echo "[4/4] PUB_HOSTED_URL=${OFFICIAL_PUB_HOSTED_URL} fvm dart pub publish --dry-run"
PUB_HOSTED_URL="$OFFICIAL_PUB_HOSTED_URL" fvm dart pub publish --dry-run

if [[ "$DO_PUBLISH" == "true" ]]; then
  echo "[PUBLISH] PUB_HOSTED_URL=${OFFICIAL_PUB_HOSTED_URL} fvm dart pub publish -f"
  PUB_HOSTED_URL="$OFFICIAL_PUB_HOSTED_URL" fvm dart pub publish -f
else
  echo "Done. Dry-run passed."
  echo "Use '.scripts/publish_pubdev.sh --publish' to publish."
fi
