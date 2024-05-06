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
  ,'fkDomain' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records that have a value in the UNIT_CONCEPT_ID field in the MEASUREMENT table that do not conform to the UNIT domain.' as check_description
  ,'MEASUREMENT' as cdm_table_name
  ,'UNIT_CONCEPT_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_fk_domain.sql' as sql_file
  ,'Conformance' as category
  ,'Value' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_fkdomain_measurement_unit_concept_id' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD_FK_DOMAIN
all standard concept ids are part of specified domain
Parameters used in this template:
cdmDatabaseSchema = sqlgen
vocabDatabaseSchema = sqlgen
cdmTableName = MEASUREMENT
cdmFieldName = UNIT_CONCEPT_ID
fkDomain = Unit
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
		  'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field, 
		  cdmTable.* 
		FROM {cdm_schema}.MEASUREMENT cdmTable
		  LEFT JOIN {cdm_schema}.concept co
		  ON cdmTable.UNIT_CONCEPT_ID = co.concept_id
		WHERE co.concept_id != 0 
		  AND co.domain_id NOT IN ('Unit')
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.MEASUREMENT cdmTable
) denominator
) cte

