create table dm_mobdi_mapping.dim_pid_device_track_android_ios_df
(
    pid       string comment '主键',
    device_list map<string,struct<abnormal_flag:int,
        devices:array<struct<device:string,time :string,cnt:int,profile_flag:int>>>>
    comment 'map<日期,struct<pid是否异常(一天对应device超过200算异常，异常置1，非异常置0), 设备号信息array<设备号，时间，前90天该device出现的天数，是否有画像>>>',
    update_time string comment 'pid的设备列表的最后更新日期'
)
    partitioned by (day string)
    stored as orc;