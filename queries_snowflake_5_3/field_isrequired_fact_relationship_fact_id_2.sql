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
  ,'isRequired' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a NULL value in the FACT_ID_2 of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
  ,'FACT_RELATIONSHIP' as cdm_table_name
  ,'FACT_ID_2' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_is_not_nullable.sql' as sql_file
  ,'Conformance' as category
  ,'Relational' as subcategory
  ,'Validation' as context
  ,'' as warning
  ,'' as error
  ,'field_isrequired_fact_relationship_fact_id_2' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD_IS_NOT_NULLABLE
For each table, check that the fields in which IS_NOT_NULLABLE == TRUE, there are no null values in that field.
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = FACT_RELATIONSHIP
cdmFieldName = FACT_ID_2
**********/
SELECT 
  num_violated_rows, 
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
		  'FACT_RELATIONSHIP.FACT_ID_2' AS violating_field, 
		  cdmTable.* 
		FROM {cdm_schema}.FACT_RELATIONSHIP cdmTable
		WHERE cdmTable.FACT_ID_2 IS NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.FACT_RELATIONSHIP cdmTable
) denominator
) cte

