-- check_name: plausibleGender
-- check_level: CONCEPT
-- checkDescription: CONCEPT
-- cdm_table_name: CONDITION_OCCURRENCE
-- cdm_field_name: CONDITION_CONCEPT_ID
-- concept_id: 4060558
-- unit_concept_id: NA
-- sql_file: concept_plausible_gender.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Validation
-- checkId: concept_plausiblegender_condition_occurrence_condition_concept_id_4060558
-- thresholdValue: 5
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleGender' AS STRING) as check_name,
  CAST('CONCEPT' AS STRING) as check_level,
  CAST('CONCEPT' AS STRING) as check_description,
  CAST('CONDITION_OCCURRENCE' AS STRING) as cdm_table_name,
  CAST('CONDITION_CONCEPT_ID' AS STRING) as cdm_field_name,
  CAST('4060558' AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('concept_plausible_gender.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Validation' AS STRING) context,
  '' as warning,
  '' as error,
  'concept_plausiblegender_condition_occurrence_condition_concept_id_4060558' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  5 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  SELECT num_violated_rows, 
 CASE 
 WHEN denominator.num_rows = 0 THEN 0 
 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT 
 COUNT(*) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT cdmTable.* 
 FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
 INNER JOIN {cdm_schema}.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.CONDITION_CONCEPT_ID = 4060558
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
 WHERE CONDITION_CONCEPT_ID = 4060558
) denominator
) cte

