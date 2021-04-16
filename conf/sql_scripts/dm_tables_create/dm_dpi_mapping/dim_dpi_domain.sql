CREATE TABLE `dm_dpi_mapping_test.dim_dpi_domain`(
                                                     `domain` string COMMENT '顶级域名，二级域名为倒序',
                                                     `level_domain` string COMMENT '域名级别')
    COMMENT '顶级域名列表'
    STORED AS orc