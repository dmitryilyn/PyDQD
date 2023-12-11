
-- check_name: plausibleDuringLife
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DRUG_EXPOSURE
-- cdm_field_name: DRUG_EXPOSURE_START_DATETIME
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_during_life.sql
-- category: Plausibility
-- subcategory: Temporal
-- context: Verification
-- checkId: field_plausibleduringlife_drug_exposure_drug_exposure_start_datetime
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'plausibleDuringLife' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'DRUG_EXPOSURE' as cdm_table_name,
  'DRUG_EXPOSURE_START_DATETIME' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_plausible_during_life.sql' as sql_file,
  'Plausibility' as category,
  'Temporal' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_plausibleduringlife_drug_exposure_drug_exposure_start_datetime' as checkid,
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATETIME' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
 JOIN {cdm_schema}.death de ON cdmTable.person_id = de.person_id
 WHERE cast(cdmTable.DRUG_EXPOSURE_START_DATETIME AS DATE) > date_add(cast(de.death_date AS DATE), 60)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
 WHERE person_id IN
 (SELECT 
 person_id 
 FROM {cdm_schema}.death)
) denominator
) cte

