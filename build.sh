#!/usr/bin/env bash
if [ $# -eq 0 ]; then
    echo "build.sh dev|pre|prod"
    exit -1
fi

DATAENGINE_HOME="$(cd "`dirname "$0"`"; pwd)"
cd ${DATAENGINE_HOME}

export MAVEN_OPTS="-Xms3096M -Xmx3096M -XX:MaxPermSize=256m"

mvn clean scalastyle:check package -P$1 -DskipTests

latest=$(readlink -f ${DATAENGINE_HOME}/)
version=$(cat ${DATAENGINE_HOME}/distribution/conf/version.properties)

if [ $1 == "pre" ] ;then
  ln -snf ${latest}/distribution/ /home/dpi_test/snapshots/${version}
elif [ $1 == "prod" ] ;then
  ln -snf ${latest}/distribution/ /home/dpi_master/tags/${version}
else
  echo "dev"
fi
#cd /home/dataengine/tags/${version}
#bash sbin/dataengine-test.sh
