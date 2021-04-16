CREATE TABLE `dm_mobdi_master.master_reserved_new`
(
    `device`            string COMMENT '设备ID',
    `pkg`               string COMMENT '未清理的应用包名',
    `refine_final_flag` int COMMENT '重新定义的flag，refine_final_flag: -1 卸载 0 在装 1新安装 2 可属于新安装安装也可以属于新卸载（2的app最终状态可能是在装，也可能是卸载）',
    `install_flag`      int COMMENT 'track到的当天的安装',
    `unstall_flag`      int COMMENT 'track到的当天的卸载',
    `final_flag`        int COMMENT '原始的final flag的定义'
)
    COMMENT '这张表每天只记录在今天安装列表有变化的设备安装列表 是一个增量表'
    PARTITIONED BY (
        `day` string COMMENT '日期')
    ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
    STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
        OUTPUTFORMAT
            'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'