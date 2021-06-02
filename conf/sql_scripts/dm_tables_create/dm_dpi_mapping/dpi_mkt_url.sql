CREATE TABLE `dm_dpi_mapping_test.dpi_mkt_url_tool_record`
(
    `plat`        string COMMENT '业务线',
    `url`         string COMMENT '原始url',
    `url_key`     string COMMENT '关键字',
    `source_type` string COMMENT '来源类型',
    `os`          string COMMENT 'ios/android',
    `cate_l1`     string COMMENT '业务类型',
    `period`      string COMMENT '业务周期',
    `url_action`  string COMMENT 'url行为',
    `describe_1`  string COMMENT 'url描述1',
    `describe_2`  string COMMENT 'url描述2',
    `id`          string COMMENT '分组id',
    `date`        string COMMENT '提交日期'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录')
    stored as orc;


CREATE TABLE `dm_dpi_mapping_test.dpi_mkt_url_tool_tag`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT 'url',
    `url_regexp`    string COMMENT '清洗url',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key'
)
    PARTITIONED BY (
        `carrier` string COMMENT '运营商',
        `version` string COMMENT 'day')
    stored as orc;

CREATE TABLE `dm_dpi_mapping_test.dm_dpi_mkt_url_tag_v2`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT 'url',
    `url_regexp`    string COMMENT '清洗url',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录')
    stored as orc;

CREATE TABLE `dm_dpi_mapping_test.dm_dpi_mkt_tag_init`
(
    `tag`            string,
    `rn`             int,
    `tag_jiangsu`    string,
    `tag_new_jangsu` string
)
    stored as orc;


CREATE TABLE `dm_dpi_mapping_test.tmp_url_operatorstag`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `plat`          string COMMENT '业务线',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期'
)
    PARTITIONED BY (
        `carrier` string COMMENT '运营商',
        `version` string COMMENT '版本记录')
    stored as orc;

CREATE TABLE `dm_dpi_mapping_test.dpi_mkt_url_withtag`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `plat`          string COMMENT '业务线',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录')
    stored as orc;

CREATE TABLE `dm_dpi_mapping_test.dpi_mkt_url_tool_first_filter`
(
    `pattern`       string COMMENT '输出格式',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `plat`          string COMMENT '业务线',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `is_regexp`     string COMMENT '匹配模式'
)
    PARTITIONED BY (
        `carrier` string COMMENT '运营商',
        `version` string COMMENT '版本记录')
    stored as orc;


CREATE TABLE `dm_dpi_mapping_test.dpi_mkt_url_tool_matcher_pattern`
(
    `tag`     string Comment 'tag',
    `tag_group_id` string Comment 'tag 分组',
    `pattern` string COMMENT 'matcher_pattern'
)
    PARTITIONED BY (
        `carrier` string COMMENT '运营商',
        `version` string COMMENT '版本记录')
    stored as orc;


CREATE TABLE `dm_dpi_mapping_test.marketplus_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.fin_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.mobeye_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.ga_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.di_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.sjhz_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');

CREATE TABLE `dm_dpi_mapping_test.zy_tag_url_mapping`
(
    `tag`           string COMMENT 'tag_id',
    `url`           string COMMENT '原始url',
    `url_regexp`    string COMMENT '清洗后URL',
    `protocal_type` string COMMENT '协议',
    `root_domain`   string COMMENT 'root',
    `host`          string COMMENT 'host',
    `file`          string COMMENT 'file',
    `path`          string COMMENT 'path',
    `query0`        string COMMENT 'query0',
    `url_key`       string COMMENT 'url_key',
    `source_type`   string COMMENT '来源',
    `os`            string COMMENT 'iOS/Anorid',
    `cate_l1`       string COMMENT '业务类型',
    `period`        string COMMENT '业务周期',
    `url_action`    string COMMENT 'url行为',
    `describe_1`    string COMMENT '描述1',
    `describe_2`    string COMMENT '描述2',
    `id`            string COMMENT '分组id',
    `date`          string COMMENT '提交日期',
    `plat`          string COMMENT '业务线'
)
    PARTITIONED BY (
        `version` string COMMENT '版本记录');