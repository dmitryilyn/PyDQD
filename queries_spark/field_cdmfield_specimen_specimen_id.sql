
-- check_name: cdmField
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: SPECIMEN
-- cdm_field_name: SPECIMEN_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_cdm_field.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_cdmfield_specimen_specimen_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'cdmField' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'SPECIMEN' as cdm_table_name,
  'SPECIMEN_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_cdm_field.sql' as sql_file,
  'Conformance' as category,
  'Relational' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_cdmfield_specimen_specimen_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  0 as threshold_value,
  NULL as notes_value
FROM (
  SELECT num_violated_rows, 
 CASE 
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE 
 WHEN COUNT(SPECIMEN_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM {cdm_schema}.SPECIMEN cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte

