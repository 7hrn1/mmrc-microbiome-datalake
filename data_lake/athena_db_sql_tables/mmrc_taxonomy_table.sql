CREATE TABLE mmrc_taxonomy_table AS 
  taxonomy_id string,
  source_file string,
  batch_year string,
  batch_month string,
  batch_day string,
  clade string,
  species string,
  strain string,
  superkingdom string,
  phylum string,
  class string,
  family string,
  genus string,
  taxon string,
  order string)
LOCATION 's3://mmrc-taxonomy-tables/staging'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);