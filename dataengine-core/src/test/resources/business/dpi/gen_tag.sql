INSERT OVERWRITE TABLE dm_dpi_mapping_test.dm_dpi_mkt_url_tag_v2 PARTITION (operator = '$operator', version = '$version')
SELECT s1.tag,
       t1.url,
       t1.url_regexp,
       t1.protocal_type,
       t1.root_domain,
       t1.host,
       t1.file,
       t1.path,
       t1.query0,
       t1.url_key
FROM (
         SELECT s.tag, s.rn, s.rn - min(s.rn) over () + 1 as rn_new
         FROM dm_dpi_mapping_test.dm_dpi_mkt_tag_init s
                  LEFT JOIN dm_dpi_mapping_test.dm_dpi_mkt_url_tag_v2 t
                            ON s.tag = t.tag
         WHERE t.tag is null
     ) s1
         JOIN (
    select a.url
         , a.url_regexp
         , a.root_domain
         , a.protocal_type
         , a.host
         , a.file
         , a.path
         , a.query0
         , a.url_key
         , a.id
         , dense_rank() over (order by id) as id_new
    from dpi_tb3 a
) t1
              on s1.rn_new = t1.id_new;

-- 每次提交，整合到一张表, 变成分区表
INSERT OVERWRITE TABLE dm_dpi_mapping_test.dpi_mkt_url_withtag PARTITION (operator = '$operator', version = '$version')
SELECT b.tag,
       a.url,
       a.url_regexp,
       a.protocal_type,
       a.root_domain,
       a.host,
       a.file,
       a.path,
       a.query0,
       a.url_key,
       a.plat,
       a.source_type,
       a.os,
       a.cate_l1,
       a.period,
       a.url_action,
       a.describe_1,
       a.describe_2,
       a.id,
       a.date
FROM dpi_tb3 a
         JOIN (
    SELECT *
    FROM dm_dpi_mapping_test.dm_dpi_mkt_url_tag_v2
    WHERE operator = '$operator'
      and version = '$version'
) b
ON a.url_regexp = b.url_regexp and a.url_key = b.url_key
GROUP BY b.tag, a.url, a.url_regexp, a.protocal_type, a.root_domain, a.host, a.file, a.path, a.query0, a.url_key,
         a.plat, a.source_type, a.os, a.cate_l1, a.period, a.url_action, a.describe_1, a.describe_2, a.id, a.date;