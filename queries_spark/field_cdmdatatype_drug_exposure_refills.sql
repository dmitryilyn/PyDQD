
-- check_name: cdmDatatype
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DRUG_EXPOSURE
-- cdm_field_name: REFILLS
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_cdm_datatype.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_cdmdatatype_drug_exposure_refills
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'cdmDatatype' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'DRUG_EXPOSURE' as cdm_table_name,
  'REFILLS' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_cdm_datatype.sql' as sql_file,
  'Conformance' as category,
  'Value' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_cdmdatatype_drug_exposure_refills' as checkid,
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
 'DRUG_EXPOSURE.REFILLS' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
 WHERE CASE WHEN CAST(abs(cdmTable.REFILLS) AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
 AND cdmTable.REFILLS IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.DRUG_EXPOSURE
) denominator
) cte

