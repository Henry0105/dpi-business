#!/usr/bin/env bash

if [ $# -eq 0 ]; then
    echo "version-build.sh {version}"
    exit -1
fi
PROJECT_VERSION=$1
DATAENGINE_HOME="$(cd "`dirname "$0"`"; pwd)"

cd ${DATAENGINE_HOME}

echo "__version__ = \"$PROJECT_VERSION\"" > sbin/main/module/const/version.py

mvn versions:set -DnewVersion=${PROJECT_VERSION}

mvn versions:commit

#git tag ${PROJECT_VERSION}
#git push origin ${PROJECT_VERSION}
