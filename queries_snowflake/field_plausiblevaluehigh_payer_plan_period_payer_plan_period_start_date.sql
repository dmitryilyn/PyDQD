-- check_name: plausibleValueHigh
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PAYER_PLAN_PERIOD
-- cdm_field_name: PAYER_PLAN_PERIOD_START_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_value_high.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: field_plausiblevaluehigh_payer_plan_period_payer_plan_period_start_date
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleValueHigh' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('PAYER_PLAN_PERIOD' AS STRING) as cdm_table_name,
  CAST('PAYER_PLAN_PERIOD_START_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_plausible_value_high.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_plausiblevaluehigh_payer_plan_period_payer_plan_period_start_date' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  1 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
PLAUSIBLE_VALUE_HIGH
get number of records and the proportion to total number of eligible records that exceed this threshold
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = PAYER_PLAN_PERIOD
cdmFieldName = PAYER_PLAN_PERIOD_START_DATE
plausibleValueHigh = DATEADD(dd,1,GETDATE())
**********/
SELECT 
	num_violated_rows, 
	CASE 
		WHEN denominator.num_rows = 0 THEN 0 
		ELSE 1.0*num_violated_rows/denominator.num_rows 
	END AS pct_violated_rows, 
  	denominator.num_rows AS num_denominator_rows
FROM
(
	SELECT 
		COUNT(violated_rows.violating_field) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT 'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_START_DATE' AS violating_field, 
		cdmTable.*
    	FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
      	WHERE cast(cdmTable.PAYER_PLAN_PERIOD_START_DATE as date) > cast(DATEADD(dd,1,CURRENT_DATE) as date)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
  	WHERE PAYER_PLAN_PERIOD_START_DATE IS NOT NULL
) denominator


) cte

