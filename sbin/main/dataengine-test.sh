#!/usr/bin/env bash

set -x -e
pwd
if [ `command -v python` ]; then
    RUNNER="python"
else
    echo "python is not install" >&2
    exit 1
fi

if [ -z "${DATAENGINE_HOME}" ]; then
    export DATAENGINE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

MID_ENGINE_PY_HOME="$DATAENGINE_HOME/sbin"
MID_ENGINE_TMP="$DATAENGINE_HOME/tmp"
MID_ENGINE_LOG_DIR="$DATAENGINE_HOME/logs"
MID_ENGINE_CONF_DIR="$DATAENGINE_HOME/conf"
MID_ENGINE_DOCS_DIR="$DATAENGINE_HOME/docs"
MID_ENGINE_UNITTEST_DIR="$DATAENGINE_HOME/unittest"

source ${MID_ENGINE_CONF_DIR}/dataengine-env.sh

if [ ! -d "$MID_ENGINE_LOG_DIR" ]; then
    mkdir -p "$MID_ENGINE_LOG_DIR"
fi

if [ ! -d "$MID_ENGINE_TMP" ]; then
    mkdir -p "$MID_ENGINE_TMP"
fi


for file in $(find ${MID_ENGINE_UNITTEST_DIR}/ -name "*.json")
do
    echo "*******************************$file******************************"
    ${RUNNER} "$MID_ENGINE_PY_HOME/driver.py" "job" "file://${file}"
    echo -e "\n\n\n"
done
