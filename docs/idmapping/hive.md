# hive 表结构

## ios_id_mapping
|Fields|Desc|Type|Comment|
|---|---|---|---|
|device|---|string|---|
|imei|---|string|','分割|
|imei_14|---|string|','分割|
|idfa|---|string|','分割|
|mac|---|string|','分割|
|phone|---|string|','分割|
|imsi|---|string|','分割|


## android_id_mapping
|Fields|Desc|Type|Comment|
|---|---|---|---|
|device|---|string|---|
|imei|---|string|','分割|
|imei_14|---|string|','分割|
|idfa|---|string|','分割|
|mac|---|string|','分割|
|phone|---|string|','分割|
|imsi|---|string|','分割|

## imei_devices_mapping

|Fields|Desc|Type|Comment|
|---|---|---|---|
|imei||string||
|imei_md5|imei_md5|string|imei_md5|
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|


## imei14_devices_mapping

|Fields|Desc|Type|Comment|
|---|---|---|---|
|imei14|imei14|string|imei14|
|imei14_md5|imei14_md5|string|imei14_md5|
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|



## mac_devices_mapping

|Fields|Desc|Type|Comment|
|---|---|---|---|
|mac|mac|string||
|mac_md5|mac_md5|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|


## idfa_devices_mapping

|Fields|Desc|Type|Comment|
|---|---|---|---|
|idfa|idfa|string||
|idfa_md5|idfa_md5|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|


## phone_devices_mapping
|Fields|Desc|Type|Comment|
|---|---|---|---|
|phone|phone|string||
|phone_md5|phone_md5|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|


## imsi_devices_mapping
|Fields|Desc|Type|Comment|
|---|---|---|---|
|imsi|imsi|string||
|imsi_md5|imsi_md5|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|update_time|device最新更新时间|string|例如'20180721'|
|day|时间分区|string|例如'20180721'|
|plat|ios/Android分区|string|1:Android,2:ios|



