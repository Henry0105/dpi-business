INSERT OVERWRITE TABLE dm_dpi_mapping_test.dpi_mkt_url_matcher_pattern PARTITION (operator='$operator', version = '$version')
SELECT tag, lower(concat_ws('\\\\;m\\\\;o\\\\;b\\\\;',tag,'like',concat_ws(',',collect_set(pattern)))) as pattern
FROM (
      SELECT tag, if(url_key = '', url_regexp, concat(host,'',path,'|',url_key))as pattern
      FROM dm_dpi_mapping_test.dpi_mkt_url_withtag
      WHERE operator = '$operator' and version = '$version'
     ) a
GROUP BY tag