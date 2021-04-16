# hbase 表结构

{day}为变量,方便后面重新切换新表

## id_mapping_{day}

|Fields|Desc|Type|Comment|
|---|---|---|---|
|device|rowkey|string||
|imei|---|string|','分割|
|imei_tm|---|string|imei最新时间|
|imei_14|---|string|','分割|
|imei_14_tm|---|string|imei最新时间|
|idfa|---|string|','分割|
|idfa_tm|---|string|idfa最新时间|
|mac|---|string|','分割|
|mac_tm|---|string|mac最新时间|
|phone|---|string|','分割|
|phone_tm|---|string|phone最新时间|
|imsi|---|string|','分割|
|imsi_tm|---|string|imsi最新时间|
|plat|ios/Android分区|string|1:Android,2:ios|


## imei_devices_mapping_{day}

|Fields|Desc|Type|Comment|
|---|---|---|---|
|imei_md5|rowkey|string|imei_md5|
|imei||string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|


## imei14_devices_mapping_{day}

|Fields|Desc|Type|Comment|
|---|---|---|---|
|imei14_md5|rowkey|string|imei14_md5|
|imei14|imei14|string|imei14|
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|



## mac_devices_mapping_{day}

|Fields|Desc|Type|Comment|
|---|---|---|---|
|mac_md5|rowkey|string||
|mac|mac|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|


## idfa_devices_mapping_{day}

|Fields|Desc|Type|Comment|
|---|---|---|---|
|idfa_md5|rowkey|string||
|idfa|idfa|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|


## phone_devices_mapping_{day}
|Fields|Desc|Type|Comment|
|---|---|---|---|
|phone_md5|rowkey|string||
|phone|phone|string||
|device|device|string|','分割|
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|


## imsi_devices_mapping_{day}
|Fields|Desc|Type|Comment|
|---|---|---|---|
|imsi_md5|rowkey|string||
|imsi|imsi|string||
|device|device|string|','分割 |
|device_tm|与device一一对应|string|','分割,与device顺序对应|
|plat|ios/Android分区|string|1:Android,2:ios|



