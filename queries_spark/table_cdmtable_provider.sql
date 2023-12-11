-- check_name: cdmTable
-- check_level: TABLE
-- checkDescription: TABLE
-- cdm_table_name: PROVIDER
-- cdm_field_name: NA
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: table_cdm_table.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: table_cdmtable_provider
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('cdmTable' AS STRING) as check_name,
  CAST('TABLE' AS STRING) as check_level,
  CAST('TABLE' AS STRING) as check_description,
  CAST('PROVIDER' AS STRING) as cdm_table_name,
  CAST(NULL AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('table_cdm_table.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Relational' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'table_cdmtable_provider' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  SELECT num_violated_rows, 
 CASE 
 WHEN denominator.num_rows = 0 THEN 0 
 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT 
 num_violated_rows 
 FROM
 (
 SELECT
 CASE 
 WHEN COUNT(*) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM {cdm_schema}.PROVIDER cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte

