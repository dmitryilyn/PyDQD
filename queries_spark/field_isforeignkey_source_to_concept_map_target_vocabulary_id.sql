-- check_name: isForeignKey
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: SOURCE_TO_CONCEPT_MAP
-- cdm_field_name: TARGET_VOCABULARY_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: is_foreign_key.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_isforeignkey_source_to_concept_map_target_vocabulary_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('isForeignKey' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('SOURCE_TO_CONCEPT_MAP' AS STRING) as cdm_table_name,
  CAST('TARGET_VOCABULARY_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('is_foreign_key.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Relational' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_isforeignkey_source_to_concept_map_target_vocabulary_id' as checkid,
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
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'SOURCE_TO_CONCEPT_MAP.TARGET_VOCABULARY_ID' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.SOURCE_TO_CONCEPT_MAP cdmTable
 LEFT JOIN {cdm_schema}.VOCABULARY fkTable
 ON cdmTable.TARGET_VOCABULARY_ID = fkTable.VOCABULARY_ID
 WHERE fkTable.VOCABULARY_ID IS NULL 
 AND cdmTable.TARGET_VOCABULARY_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.SOURCE_TO_CONCEPT_MAP cdmTable
) denominator
) cte

