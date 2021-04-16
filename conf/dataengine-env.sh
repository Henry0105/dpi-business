#!/usr/bin/env bash
export MID_ENGINE_ENV=${dataengine.env}
export dataengine_shell_mock=0

# uninstall
export uninstall_mysql_user=${uninstall.mysql.user}
export uninstall_mysql_pwd=${uninstall.mysql.pwd}
export uninstall_mysql_host=${uninstall.mysql.host}
export uninstall_mysql_port=${uninstall.mysql.port}
export uninstall_url=${uninstall.url}
export uninstall_es_path=${uninstall.es.path}

source $DATAENGINE_HOME/conf/hive_database_table.properties