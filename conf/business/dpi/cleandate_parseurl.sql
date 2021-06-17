-- 数据清洗
CREATE OR REPLACE TEMPORARY VIEW dpi_tb1 as
SELECT plat,
       if(substring(url,length(url),1)='/', substring(url,1,length(url)-1), url) as url,
       url_key,
       source_type,
       os,
       cate_l1,
       period,
       url_action,
       describe_1,
       describe_2,
       id,
       date
FROM (
      SELECT trim(plat)          as plat,
             regexp_replace(trim(url),'\"|\\*$','') as url,
             trim(url_key)       as url_key,
             trim(source_type)   as source_type,
             trim(os)            as os,
             trim(cate_l1)       as cate_l1,
             trim(period)        as period,
             trim(url_action)    as url_action,
             trim(describe_1)    as describe_1,
             trim(describe_2)    as describe_2,
             trim(id)            as id,
             trim(date)          as date
      FROM dpi_input
     ) a;


-- 解析URL
CREATE OR REPLACE TEMPORARY VIEW dpi_tb2 as
SELECT url,
       protocal_type,
       regexp_replace(host,'^\\*|^\\.|\\*$|\\.$','') as host,
       regexp_replace(file,'^\\/\\*','/') as file,
       regexp_replace(path,'^\\/\\*','/') as path,
       regexp_replace(query0,'^\\*|\\*$','') as query0,
       url_key,
       plat,
       source_type,
       os,
       cate_l1,
       period,
       url_action,
       describe_1,
       describe_2,
       id,
       date
FROM (
      SELECT url,
             regexp_extract(url,'^(https|http)') as protocal_type,
             if(regexp_extract(url,'^(https|http)')!='',parse_url(url,'HOST'), parse_url(concat('http://',url), 'HOST'))  as host,
             if(regexp_extract(url,'^(https|http)')!='',parse_url(url,'FILE'), parse_url(concat('http://',url), 'FILE'))  as file,
             if(regexp_extract(url,'^(https|http)')!='',parse_url(url,'PATH'), parse_url(concat('http://',url), 'PATH'))  as path,
             if(regexp_extract(url,'^(https|http)')!='',parse_url(url,'QUERY'),parse_url(concat('http://',url), 'QUERY')) as query0,
             url_key,
             plat,
             source_type,
             os,
             cate_l1,
             period,
             url_action,
             describe_1,
             describe_2,
             id,
             date
      FROM dpi_tb1
     ) a;


-- 获取root domain
CREATE OR REPLACE TEMPORARY VIEW dpi_tb3 as
SELECT url,
       concat(host,'',file) as url_regexp,
       protocal_type,
       case when t2.domain is not null then root_domain_2
            when t1.domain is not null then root_domain_1
            else host
       end as root_domain,
       host,
       file,
       path,
       query0,
       url_key,
       plat,
       source_type,
       os,
       cate_l1,
       period,
       url_action,
       describe_1,
       describe_2,
       id,
       date
FROM (
      SELECT plat,
             url,
             protocal_type,
             host,
             file,
             path,
             query0,
             url_key,
             reverse(split(reverse(host),'\\.')[0]) as host_rev_0,
             concat_ws('.',reverse(split(reverse(host),'\\.')[0]),reverse(split(reverse(host),'\\.')[1])) as host_rev_1,
             concat_ws('.',reverse(split(reverse(host),'\\.')[1]),reverse(split(reverse(host),'\\.')[0])) as root_domain_1,
             case
                 when size(split(reverse(host),'\\.'))>2
                 then concat_ws('.',reverse(split(reverse(host),'\\.')[2]),reverse(split(reverse(host),'\\.')[1]),reverse(split(reverse(host),'\\.')[0]))
                 else ''
             end as root_domain_2,
             source_type,
             os,
             cate_l1,
             period,
             url_action,
             describe_1,
             describe_2,
             id,
             date
      FROM dpi_tb2
     ) s
LEFT JOIN $dim_domain t1
ON host_rev_0 = t1.domain
LEFT JOIN $dim_domain t2
ON host_rev_1 = t2.domain