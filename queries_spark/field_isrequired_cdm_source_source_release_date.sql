
-- check_name: isRequired
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: CDM_SOURCE
-- cdm_field_name: SOURCE_RELEASE_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_is_not_nullable.sql
-- category: Conformance
-- subcategory: Relational
-- context: Validation
-- checkId: field_isrequired_cdm_source_source_release_date
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'isRequired' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'CDM_SOURCE' as cdm_table_name,
  'SOURCE_RELEASE_DATE' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_is_not_nullable.sql' as sql_file,
  'Conformance' as category,
  'Relational' as subcategory,
  'Validation' as context,
  '' as warning,
  '' as error,
  'field_isrequired_cdm_source_source_release_date' as checkid,
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
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'CDM_SOURCE.SOURCE_RELEASE_DATE' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.CDM_SOURCE cdmTable
 WHERE cdmTable.SOURCE_RELEASE_DATE IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.CDM_SOURCE cdmTable
) denominator
) cte

