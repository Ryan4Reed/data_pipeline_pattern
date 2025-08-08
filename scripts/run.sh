#!/usr/bin/env bash
set -euo pipefail
JOB_CLASS="$1"; shift || true
./gradlew :runner:shadowJar
spark-submit --class "${JOB_CLASS}" modules/runner/build/libs/runner-all.jar "$@"