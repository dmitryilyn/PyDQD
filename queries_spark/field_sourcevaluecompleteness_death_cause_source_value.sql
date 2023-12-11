
-- check_name: sourceValueCompleteness
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DEATH
-- cdm_field_name: CAUSE_SOURCE_VALUE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_source_value_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Verification
-- checkId: field_sourcevaluecompleteness_death_cause_source_value
-- thresholdValue: 10
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'sourceValueCompleteness' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'DEATH' as cdm_table_name,
  'CAUSE_SOURCE_VALUE' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_source_value_completeness.sql' as sql_file,
  'Completeness' as category,
  '' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_sourcevaluecompleteness_death_cause_source_value' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  10 as threshold_value,
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
 FROM (
 /*violatedRowsBegin*/
 SELECT DISTINCT 
 'DEATH.CAUSE_SOURCE_VALUE' AS violating_field, 
 cdmTable.CAUSE_SOURCE_VALUE
 FROM {cdm_schema}.DEATH cdmTable
 WHERE cdmTable.CAUSE_SOURCE_CONCEPT_ID = 0
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(distinct cdmTable.CAUSE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.CAUSE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
 FROM {cdm_schema}.DEATH cdmTable
) denominator
) cte

