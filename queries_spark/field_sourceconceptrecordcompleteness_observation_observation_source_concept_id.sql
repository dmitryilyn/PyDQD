-- check_name: sourceConceptRecordCompleteness
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: OBSERVATION
-- cdm_field_name: OBSERVATION_SOURCE_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_concept_record_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Verification
-- checkId: field_sourceconceptrecordcompleteness_observation_observation_source_concept_id
-- thresholdValue: 100
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('sourceConceptRecordCompleteness' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('OBSERVATION' AS STRING) as cdm_table_name,
  CAST('OBSERVATION_SOURCE_CONCEPT_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_concept_record_completeness.sql' AS STRING) as sql_file,
  CAST('Completeness' AS STRING) as category,
  CAST('' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_sourceconceptrecordcompleteness_observation_observation_source_concept_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  100 as threshold_value,
  CAST(NULL AS STRING) as notes_value
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
 'OBSERVATION.OBSERVATION_SOURCE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.OBSERVATION cdmTable
 WHERE cdmTable.OBSERVATION_SOURCE_CONCEPT_ID = 0 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM {cdm_schema}.OBSERVATION cdmTable
) denominator
) cte

