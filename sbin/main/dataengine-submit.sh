#!/bin/bash
: '
@owner: zhangjt
@describe: 中间件执行入口
@projectName: data_engine
@BusinessName: todo
@SourceTable:
@TargetTable:
@TableRelation:
@DependFilePath:
'

set -x -e
pwd
if [ `command -v python` ]; then
    RUNNER="python"
else
    echo "python is not install" >&2
    exit 1
fi

# mac os readlink -f not work
if [ -z "${DATAENGINE_HOME}" ]; then
    export DATAENGINE_HOME="$(readlink -f $(cd "`dirname "$0"`"/..; pwd))"
fi

MID_ENGINE_PY_HOME="$DATAENGINE_HOME/sbin"
MID_ENGINE_TMP="$DATAENGINE_HOME/tmp"
MID_ENGINE_LOG_DIR="$DATAENGINE_HOME/logs"
MID_ENGINE_CONF_DIR="$DATAENGINE_HOME/conf"

source ${MID_ENGINE_CONF_DIR}/dataengine-env.sh

if [ ! -d "$MID_ENGINE_LOG_DIR" ]; then
    mkdir -p "$MID_ENGINE_LOG_DIR"
fi

if [ ! -d "$MID_ENGINE_TMP" ]; then
    mkdir -p "$MID_ENGINE_TMP"
fi

${RUNNER} "$MID_ENGINE_PY_HOME/driver.py" "job" "$@"