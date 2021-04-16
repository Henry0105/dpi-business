-- departed
CREATE TABLE IF NOT EXISTS `cleaned_device_limited`(
  `device` string)
PARTITIONED BY (
  `day` string,
  `userid` string) STORED AS orc;