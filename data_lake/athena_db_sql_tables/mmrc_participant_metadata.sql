CREATE TABLE mmrc_participant_metadata AS 
  sample_id string,
  source_file string,
  batch_year string,
  batch_month string,
  batch_day string,
  by string,
  age string,
  psqi string,
  gender string,
  gpaq string,
  ethnicity string,
  agegroup string,
  state string,
  sago string)
LOCATION 's3://mmrc-participant-metadata/staging'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);