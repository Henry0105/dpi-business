CREATE TABLE dm_dataengine_tags.profile_tags_info_full
(
    device     string COMMENT '设备',
    tags       map<string, array<string>> COMMENT '标签id和array(值, update_time)的map',
    confidence map<string, array<string>> COMMENT '标签id和array(置信度, update_time)的map'
)
    PARTITIONED BY (
        day string COMMENT '日期'
        )
    STORED AS ORC;