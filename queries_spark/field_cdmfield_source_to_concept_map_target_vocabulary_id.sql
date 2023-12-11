-- check_name: cdmField
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: SOURCE_TO_CONCEPT_MAP
-- cdm_field_name: TARGET_VOCABULARY_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_cdm_field.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_cdmfield_source_to_concept_map_target_vocabulary_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('cdmField' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('SOURCE_TO_CONCEPT_MAP' AS STRING) as cdm_table_name,
  CAST('TARGET_VOCABULARY_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_cdm_field.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Relational' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_cdmfield_source_to_concept_map_target_vocabulary_id' as checkid,
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
 WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT num_violated_rows FROM (
 SELECT
 CASE 
 WHEN COUNT(TARGET_VOCABULARY_ID) = 0 THEN 0
 ELSE 0
 END AS num_violated_rows
 FROM {cdm_schema}.SOURCE_TO_CONCEPT_MAP cdmTable
 ) violated_rows
) violated_row_count cross join (SELECT 1 AS num_rows
) denominator
) cte

