--DDL to create dqdashboard_results table.
DROP TABLE IF EXISTS sqlgen.dqdashboard_results;
CREATE TABLE sqlgen.dqdashboard_results
(
  num_violated_rows     BIGINT,
  pct_violated_rows     FLOAT,
  num_denominator_rows  BIGINT,
  execution_time        VARCHAR(255),
  query_text            VARCHAR(8000),
  check_name            VARCHAR(255),
  check_level           VARCHAR(255),
  check_description     VARCHAR(8000),
  cdm_table_name        VARCHAR(255),
  cdm_field_name        VARCHAR(255),
  concept_id            VARCHAR(255),
  unit_concept_id       VARCHAR(255),
  sql_file              VARCHAR(255),
  category              VARCHAR(255),
  subcategory           VARCHAR(255),
  context               VARCHAR(255),
  warning               VARCHAR(255),
  error                 VARCHAR(8000),
  checkid               VARCHAR(1024),
  is_error              INTEGER,
  not_applicable        INTEGER,
  failed                INTEGER,
  passed                INTEGER,
  not_applicable_reason VARCHAR(8000),
  threshold_value       INTEGER,
  notes_value           VARCHAR(8000)
);

