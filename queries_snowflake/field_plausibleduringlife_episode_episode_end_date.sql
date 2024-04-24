-- check_name: plausibleDuringLife
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: EPISODE
-- cdm_field_name: EPISODE_END_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_plausible_during_life.sql
-- category: Plausibility
-- subcategory: Temporal
-- context: Verification
-- checkId: field_plausibleduringlife_episode_episode_end_date
-- thresholdValue: 1
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleDuringLife' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('EPISODE' AS STRING) as cdm_table_name,
  CAST('EPISODE_END_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_plausible_during_life.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Temporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_plausibleduringlife_episode_episode_end_date' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  1 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
PLAUSIBLE_DURING_LIFE
get number of events that occur after death event (PLAUSIBLE_DURING_LIFE == Yes)
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = EPISODE
cdmFieldName = EPISODE_END_DATE
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
		SELECT 
			'EPISODE.EPISODE_END_DATE' AS violating_field, 
			cdmTable.*
    	FROM {cdm_schema}.EPISODE cdmTable
    	JOIN {cdm_schema}.death de ON cdmTable.person_id = de.person_id
    	WHERE cast(cdmTable.EPISODE_END_DATE AS DATE) > DATEADD(day, 60, cast(de.death_date AS DATE))
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.EPISODE cdmTable
	WHERE person_id IN
		(SELECT 
			person_id 
		FROM {cdm_schema}.death)
) denominator


) cte

