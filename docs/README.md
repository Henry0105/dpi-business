# 脚本列表
在`sbin/main`下的脚本描述:

| script | desc |
|---|---|
| dataengine-submit.sh | 大数据任务的总入口 |
| dataengine-test.sh | 用于跑单元测试 |
| dataengine-tools.sh | 中间件各个工具的入口(上传dfs, idmapping, 安卓标签,ios标签) |
| module/events/crowd.py | 人群包优选 |
| module/events/filter.py | 人群交并差, 人群包二次删选 |
| module/events/idmapping.py | idmapping任务入口, 包含导hbase |
| module/events/lookalike.py | lookalike |
| module/events/mapping.py | idmapping离线匹配任务,地理围栏过滤device |
| module/events/pca_tfidf.py | PCATfidf定时任务 |
| module/events/portrait.py | 自定义群体画像标签计算/推算/美化 |
| module/events/tags.py | 安卓/ios标签生成,包含导hbase |
| module/rpc | 中间件内部自用rpc服务 |
| module/test | 一些test脚本 |
| module/tools/cfg.py | 载入配置 |
| module/tools/dfs.py | dfs上传/下载 |
| module/tools/env.py | 中间件环境载入 |
| module/tools/hadoop.py | hadoop相关工具(distcp) |
| module/tools/hbase.py | hbase相关操作(增量导入,删除) |
| module/tools/hdfs.py | hdfs相关操作(获取当前可以namenode,检查hdfs路径是否存在) |
| module/tools/hfile.py | hbase bulkLoad调用的封装 |
| module/tools/hive.py | hive调用的封装 |
| module/tools/http.py | httpclient调用的封装 |
| module/tools/java.py | java调用的封装 |
| module/tools/notify.py | 发邮件,通过rabbitmq发送消息 |
| module/tools/opt.py | 解析保存dfs文件参数的类 |
| module/tools/shell.py | shell调用的封装 |
| module/tools/spark.py | spark调用的封装 |
| module/tools/tar.py | 打tar.gz包 |
| module/tools/utils.py | utils |
| sdk_plus/score_uninstall.sh | 卸载预测模型预测部分,包含导es |
| sdk_plus/uninstall_stat_all.sh | 统计appkey的每日卸载量 |
| sdk_plus/uninstall_stat_daily.sh | 统计appkey的总卸载量 |
| sdk_plus/uninstall_stat_weekly.sh | 统计appkey的周卸载量 |
