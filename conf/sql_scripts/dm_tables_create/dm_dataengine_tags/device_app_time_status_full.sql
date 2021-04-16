CREATE TABLE dm_dataengine_tags.device_app_time_status_full
(
    device      string COMMENT '设备id',
    pkg         string COMMENT 'app包名,清洗前',
    app_name    string COMMENT '应用名称',
    installed   array<int> COMMENT '在装状态bitDate',
    active      array<int> COMMENT '活跃状态bitDate',
    uninstall   array<int> COMMENT '卸载状态bitDate',
    new_install array<int> COMMENT '新安装状态bitDate',
    upper_time  string COMMENT '最迟更新时间'
)
    COMMENT 'app状态时间维度记录全量表'
    PARTITIONED BY (
        day string COMMENT '入库日期')
    stored as orc