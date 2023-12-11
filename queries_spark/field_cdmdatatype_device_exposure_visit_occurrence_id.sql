-- check_name: cdmDatatype
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DEVICE_EXPOSURE
-- cdm_field_name: VISIT_OCCURRENCE_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_cdm_datatype.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_cdmdatatype_device_exposure_visit_occurrence_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('cdmDatatype' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('DEVICE_EXPOSURE' AS STRING) as cdm_table_name,
  CAST('VISIT_OCCURRENCE_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_cdm_datatype.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Value' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_cdmdatatype_device_exposure_visit_occurrence_id' as checkid,
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'DEVICE_EXPOSURE.VISIT_OCCURRENCE_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.DEVICE_EXPOSURE cdmTable
 WHERE CASE WHEN CAST(abs(cdmTable.VISIT_OCCURRENCE_ID) AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
 AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.DEVICE_EXPOSURE
) denominator
) cte

