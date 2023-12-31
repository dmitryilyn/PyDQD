-- check_name: withinVisitDates
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DRUG_EXPOSURE
-- cdm_field_name: DRUG_EXPOSURE_START_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_within_visit_dates.sql
-- category: Conformance
-- subcategory: 
-- context: Verification
-- checkId: field_withinvisitdates_drug_exposure_drug_exposure_start_date
-- thresholdValue: 5
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('withinVisitDates' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('DRUG_EXPOSURE' AS STRING) as cdm_table_name,
  CAST('DRUG_EXPOSURE_START_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_within_visit_dates.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_withinvisitdates_drug_exposure_drug_exposure_start_date' as checkid,
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
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
 JOIN {cdm_schema}.visit_occurrence vo
 ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
 WHERE cdmTable.DRUG_EXPOSURE_START_DATE < date_add(vo.visit_start_date, -7)
 OR cdmTable.DRUG_EXPOSURE_START_DATE > date_add(vo.visit_end_date, 7)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
 JOIN {cdm_schema}.visit_occurrence vo
 ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
) denominator
) cte

