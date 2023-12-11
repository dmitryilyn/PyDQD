
-- check_name: measureValueCompleteness
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: METADATA
-- cdm_field_name: VALUE_AS_STRING
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_measure_value_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Verification
-- checkId: field_measurevaluecompleteness_metadata_value_as_string
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'measureValueCompleteness' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'METADATA' as cdm_table_name,
  'VALUE_AS_STRING' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_measure_value_completeness.sql' as sql_file,
  'Completeness' as category,
  '' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_measurevaluecompleteness_metadata_value_as_string' as checkid,
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
 WHEN denominator.num_rows = 0 THEN 0 
 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT 
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'METADATA.VALUE_AS_STRING' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.METADATA cdmTable
 WHERE cdmTable.VALUE_AS_STRING IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.METADATA cdmTable
) denominator
) cte

