/*********
SQL to create query for insertion into results table. These may be unioned together prior to insert.
Note that this does not include information about SQL errors or performance.
**********/
SELECT 
  cte.num_violated_rows
  ,cte.pct_violated_rows
  ,cte.num_denominator_rows
  ,'' as execution_time
  ,'' as query_text
  ,'isPrimaryKey' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records that have a duplicate value in the OBSERVATION_PERIOD_ID field of the OBSERVATION_PERIOD.' as check_description
  ,'OBSERVATION_PERIOD' as cdm_table_name
  ,'OBSERVATION_PERIOD_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_is_primary_key.sql' as sql_file
  ,'Conformance' as category
  ,'Relational' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_isprimarykey_observation_period_observation_period_id' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD_IS_PRIMARY_KEY
Primary Key - verify those fields where IS_PRIMARY_KEY == Yes, the values in that field are unique
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = OBSERVATION_PERIOD
cdmFieldName = OBSERVATION_PERIOD_ID
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
			'OBSERVATION_PERIOD.OBSERVATION_PERIOD_ID' AS violating_field, 
			cdmTable.* 
		FROM {cdm_schema}.OBSERVATION_PERIOD cdmTable
		WHERE cdmTable.OBSERVATION_PERIOD_ID IN ( 
			SELECT 
			  OBSERVATION_PERIOD_ID 
		  FROM {cdm_schema}.OBSERVATION_PERIOD
			GROUP BY OBSERVATION_PERIOD_ID
			HAVING COUNT(*) > 1 
		)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.OBSERVATION_PERIOD cdmTable
) denominator
) cte

