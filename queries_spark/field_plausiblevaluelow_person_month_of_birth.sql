
-- check_name: plausibleValueLow
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PERSON
-- cdm_field_name: MONTH_OF_BIRTH
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_value_low.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: field_plausiblevaluelow_person_month_of_birth
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'plausibleValueLow' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'PERSON' as cdm_table_name,
  'MONTH_OF_BIRTH' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_plausible_value_low.sql' as sql_file,
  'Plausibility' as category,
  'Atemporal' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_plausiblevaluelow_person_month_of_birth' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  1 as threshold_value,
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
 'PERSON.MONTH_OF_BIRTH' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.PERSON cdmTable
 WHERE cdmTable.MONTH_OF_BIRTH < 1
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.PERSON cdmTable
 WHERE MONTH_OF_BIRTH IS NOT NULL
) denominator
) cte

