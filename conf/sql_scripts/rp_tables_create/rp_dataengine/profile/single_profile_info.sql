CREATE TABLE IF NOT EXISTS rp_dataengine.single_profile_info(
  data string,
  device string,
  tags map<string,string>,
  confidence map<string,string>)
PARTITIONED BY (
  day string,
  uuid string)
  stored as orc;

CREATE TABLE IF NOT EXISTS rp_dataengine.single_profile_track_info (
    device string,
    day string,
    features map<string,array<string>>
  )
  PARTITIONED BY (
    uuid string
  ) STORED AS orc;

CREATE TABLE IF NOT EXISTS rp_dataengine_test.rp_data_hub (
    feature map<string,array<string>>
  )
  PARTITIONED BY (
    uuid string
  ) STORED AS orc;