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
  ,'isForeignKey' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records that have a value in the COST_DOMAIN_ID field in the COST table that does not exist in the DOMAIN table.' as check_description
  ,'COST' as cdm_table_name
  ,'COST_DOMAIN_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'is_foreign_key.sql' as sql_file
  ,'Conformance' as category
  ,'Relational' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_isforeignkey_cost_cost_domain_id' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
IS_FOREIGN_KEY
Foreign key check
Parameters used in this template:
cdmDatabaseSchema = sqlgen
vocabDatabaseSchema = sqlgen
cdmTableName = COST
cdmFieldName = COST_DOMAIN_ID
fkTableName = DOMAIN
fkFieldName = DOMAIN_ID
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
		  'COST.COST_DOMAIN_ID' AS violating_field, 
		  cdmTable.*
		FROM {cdm_schema}.COST cdmTable
		  LEFT JOIN {cdm_schema}.DOMAIN fkTable
		  ON cdmTable.COST_DOMAIN_ID = fkTable.DOMAIN_ID
		WHERE fkTable.DOMAIN_ID IS NULL 
		  AND cdmTable.COST_DOMAIN_ID IS NOT NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.COST cdmTable
) denominator
) cte

