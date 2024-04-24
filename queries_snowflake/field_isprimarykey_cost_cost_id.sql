-- check_name: isPrimaryKey
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: COST
-- cdm_field_name: COST_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_is_primary_key.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_isprimarykey_cost_cost_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('isPrimaryKey' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('COST' AS STRING) as cdm_table_name,
  CAST('COST_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_is_primary_key.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Relational' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_isprimarykey_cost_cost_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
FIELD_IS_PRIMARY_KEY
Primary Key - verify those fields where IS_PRIMARY_KEY == Yes, the values in that field are unique
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = COST
cdmFieldName = COST_ID
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
			'COST.COST_ID' AS violating_field, 
			cdmTable.* 
		FROM {cdm_schema}.COST cdmTable
		WHERE cdmTable.COST_ID IN ( 
			SELECT 
			  COST_ID 
		  FROM {cdm_schema}.COST
			GROUP BY COST_ID
			HAVING COUNT(*) > 1 
		)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.COST cdmTable
) denominator

) cte

