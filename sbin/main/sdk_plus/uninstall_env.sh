#!/usr/bin/env bash

set -e -x
# mac os readlink -f not work
if [ -z "${DATAENGINE_HOME}" ]; then
    export DATAENGINE_HOME="$(readlink -f $(cd ../..; pwd))"
fi

conf_dir="$DATAENGINE_HOME/conf"
lib_dir="$DATAENGINE_HOME/lib"
source $conf_dir/dataengine-env.sh
job_jar="$lib_dir/predict-uninstall-*-jar-with-dependencies.jar"