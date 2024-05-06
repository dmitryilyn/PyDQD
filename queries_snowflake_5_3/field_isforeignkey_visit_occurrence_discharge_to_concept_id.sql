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
  ,'The number and percent of records that have a value in the DISCHARGE_TO_CONCEPT_ID field in the VISIT_OCCURRENCE table that does not exist in the CONCEPT table.' as check_description
  ,'VISIT_OCCURRENCE' as cdm_table_name
  ,'DISCHARGE_TO_CONCEPT_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'is_foreign_key.sql' as sql_file
  ,'Conformance' as category
  ,'Relational' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_isforeignkey_visit_occurrence_discharge_to_concept_id' as checkid
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
cdmTableName = VISIT_OCCURRENCE
cdmFieldName = DISCHARGE_TO_CONCEPT_ID
fkTableName = CONCEPT
fkFieldName = CONCEPT_ID
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
		  'VISIT_OCCURRENCE.DISCHARGE_TO_CONCEPT_ID' AS violating_field, 
		  cdmTable.*
		FROM {cdm_schema}.VISIT_OCCURRENCE cdmTable
		  LEFT JOIN {cdm_schema}.CONCEPT fkTable
		  ON cdmTable.DISCHARGE_TO_CONCEPT_ID = fkTable.CONCEPT_ID
		WHERE fkTable.CONCEPT_ID IS NULL 
		  AND cdmTable.DISCHARGE_TO_CONCEPT_ID IS NOT NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.VISIT_OCCURRENCE cdmTable
) denominator
) cte

