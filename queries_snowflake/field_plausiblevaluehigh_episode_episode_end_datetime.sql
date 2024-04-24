-- check_name: plausibleValueHigh
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: EPISODE
-- cdm_field_name: EPISODE_END_DATETIME
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_value_high.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: field_plausiblevaluehigh_episode_episode_end_datetime
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
  CAST('EPISODE' AS STRING) as cdm_table_name,
  CAST('EPISODE_END_DATETIME' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_plausible_value_high.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_plausiblevaluehigh_episode_episode_end_datetime' as checkid,
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
cdmTableName = EPISODE
cdmFieldName = EPISODE_END_DATETIME
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
		SELECT 'EPISODE.EPISODE_END_DATETIME' AS violating_field, 
		cdmTable.*
    	FROM {cdm_schema}.EPISODE cdmTable
      	WHERE cast(cdmTable.EPISODE_END_DATETIME as date) > cast(DATEADD(dd,1,CURRENT_DATE) as date)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.EPISODE cdmTable
  	WHERE EPISODE_END_DATETIME IS NOT NULL
) denominator


) cte

