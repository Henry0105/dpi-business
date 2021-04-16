INSERT OVERWRITE TABLE dm_dpi_mapping_test.dpi_mkt_url_first_filter_all_v3 PARTITION (operator='$operator', version='$version')
SELECT pattern, url, url_regexp, protocal_type, root_domain, host, file, path, query0, url_key,
       plat, source_type, os, cate_l1, period, url_action, describe_1, describe_2, id, date,
       '1' as is_regexp
FROM (
      SELECT concat_ws('~',root_domain,host,'','',if(protocal_type='https','',path),'1') as pattern,
             url, url_regexp, protocal_type, root_domain, host, file, path, query0, url_key,
             plat, source_type, os, cate_l1, period, url_action, describe_1, describe_2, id, date,
             '1' as is_regexp
      FROM    dm_dpi_mapping_test.dpi_mkt_url_withtag
      WHERE   operator = '$operator' and version='$version'
     ) s1;