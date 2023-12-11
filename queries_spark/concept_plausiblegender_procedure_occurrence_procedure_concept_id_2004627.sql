
-- check_name: plausibleGender
-- check_level: CONCEPT
-- checkDescription: CONCEPT
-- cdm_table_name: PROCEDURE_OCCURRENCE
-- cdm_field_name: PROCEDURE_CONCEPT_ID
-- concept_id: 2004627
-- unit_concept_id: NA
-- sql_file: concept_plausible_gender.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Validation
-- checkId: concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004627
-- thresholdValue: 5
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'plausibleGender' as check_name,
  'CONCEPT' as check_level,
  'CONCEPT' as check_description,
  'PROCEDURE_OCCURRENCE' as cdm_table_name,
  'PROCEDURE_CONCEPT_ID' as cdm_field_name,
  '2004627' as concept_id,
  'NA' as unit_concept_id,
  'concept_plausible_gender.sql' as sql_file,
  'Plausibility' as category,
  'Atemporal' as subcategory,
  'Validation' as context,
  '' as warning,
  '' as error,
  'concept_plausiblegender_procedure_occurrence_procedure_concept_id_2004627' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  5 as threshold_value,
  NULL as notes_value
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
 FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
 INNER JOIN {cdm_schema}.person p
 ON cdmTable.person_id = p.person_id
 WHERE cdmTable.PROCEDURE_CONCEPT_ID = 2004627
 AND p.gender_concept_id <> 8532 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
 WHERE PROCEDURE_CONCEPT_ID = 2004627
) denominator
) cte

