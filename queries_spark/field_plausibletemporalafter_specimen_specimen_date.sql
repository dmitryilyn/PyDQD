-- check_name: plausibleTemporalAfter
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: SPECIMEN
-- cdm_field_name: SPECIMEN_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_temporal_after.sql
-- category: Plausibility
-- subcategory: Temporal
-- context: Verification
-- checkId: field_plausibletemporalafter_specimen_specimen_date
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleTemporalAfter' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('SPECIMEN' AS STRING) as cdm_table_name,
  CAST('SPECIMEN_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_plausible_temporal_after.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Temporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_plausibletemporalafter_specimen_specimen_date' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  1 as threshold_value,
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'SPECIMEN.SPECIMEN_DATE' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.SPECIMEN cdmTable
 JOIN {cdm_schema}.PERSON plausibleTable ON cdmTable.person_id = plausibleTable.person_id
 WHERE 
 COALESCE(
 CAST(plausibleTable.BIRTH_DATETIME AS DATE),
 CAST(CONCAT(plausibleTable.year_of_birth,'-06-01') AS DATE)
 ) 
 > CAST(cdmTable.SPECIMEN_DATE AS DATE)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.SPECIMEN cdmTable
) denominator
) cte

