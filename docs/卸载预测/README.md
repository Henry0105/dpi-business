# 卸载预测生成的特征数据维度check
presto语法
```sql
select * from rp_sdkplus_uninstall.features_table
where app='171e5ee0d1eae' and day='20181020'
  and cardinality(filter(index, x -> (x >= 100 and x <= 311))) > 0
  and cardinality(filter(index, x -> (x >= 400 and x <= 611))) > 0
   and cardinality(filter(index, x -> (x >= 700 and x <= 911))) > 0
   and cardinality(filter(index, x -> (x >= 2300 and x <= 3299))) > 0
   and cardinality(filter(index, x -> (x >= 1000 and x <= 1211))) > 0
   and cardinality(filter(index, x -> (x >= 1300 and x <= 2299))) > 0
   and cardinality(filter(index, x -> (x >= 3300 and x <= 7299))) > 0
limit 100
```

# 添加新appkey时需要做以下操作
1. 更新 `rp_sdkplus_uninstall.appkey_pkg_mapping_par`
2. 更新 `rp_sdkplus_uninstall.model_device_top_app4000`
3. 更新 `rp_sdkplus_uninstall.device_sdk_run_master_2weeks`

presto检查 `rp_sdkplus_uninstall.device_sdk_run_master_2weeks`数据已经生成:
```sql
SELECT *
from rp_sdkplus_uninstall.device_sdk_run_master_2weeks
where day='20190202' and element_at(appkey_days, '286386985d080') is not null
limit 10
```