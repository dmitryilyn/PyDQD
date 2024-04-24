-- check_name: plausibleValueLow
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DRUG_EXPOSURE
-- cdm_field_name: REFILLS
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_value_low.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: field_plausiblevaluelow_drug_exposure_refills
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleValueLow' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('DRUG_EXPOSURE' AS STRING) as cdm_table_name,
  CAST('REFILLS' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_plausible_value_low.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_plausiblevaluelow_drug_exposure_refills' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  1 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
PLAUSIBLE_VALUE_LOW
get number of records and the proportion to total number of eligible records that fall below this threshold
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = DRUG_EXPOSURE
cdmFieldName = REFILLS
plausibleValueLow = 0
**********/
SELECT num_violated_rows,
  CASE
    WHEN denominator.num_rows = 0 THEN 0 
    ELSE 1.0*num_violated_rows/denominator.num_rows
  END AS pct_violated_rows,
  denominator.num_rows AS num_denominator_rows
FROM (
	SELECT 
	  COUNT(violated_rows.violating_field) AS num_violated_rows
	FROM (
		/*violatedRowsBegin*/
		SELECT 
		  'DRUG_EXPOSURE.REFILLS' AS violating_field, 
		  cdmTable.*
		FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
    WHERE cdmTable.REFILLS < 0
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.DRUG_EXPOSURE cdmTable
	WHERE REFILLS IS NOT NULL
) denominator


) cte

