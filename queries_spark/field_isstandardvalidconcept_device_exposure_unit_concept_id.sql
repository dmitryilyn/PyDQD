
-- check_name: isStandardValidConcept
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DEVICE_EXPOSURE
-- cdm_field_name: UNIT_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_is_standard_valid_concept.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_isstandardvalidconcept_device_exposure_unit_concept_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'isStandardValidConcept' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'DEVICE_EXPOSURE' as cdm_table_name,
  'UNIT_CONCEPT_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_is_standard_valid_concept.sql' as sql_file,
  'Conformance' as category,
  'Value' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_isstandardvalidconcept_device_exposure_unit_concept_id' as checkid,
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'DEVICE_EXPOSURE.UNIT_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.DEVICE_EXPOSURE cdmTable
 JOIN {cdm_schema}.concept co 
 ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND (co.standard_concept != 'S' OR co.invalid_reason IS NOT NULL)
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.DEVICE_EXPOSURE cdmTable
) denominator
) cte

