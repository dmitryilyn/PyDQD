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
  ,'plausibleValueHigh' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of records with a value in the YEAR_OF_BIRTH field of the PERSON table greater than YEAR(GETDATE())+1.' as check_description
  ,'PERSON' as cdm_table_name
  ,'YEAR_OF_BIRTH' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_plausible_value_high.sql' as sql_file
  ,'Plausibility' as category
  ,'Atemporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_plausiblevaluehigh_person_year_of_birth' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,1 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
PLAUSIBLE_VALUE_HIGH
get number of records and the proportion to total number of eligible records that exceed this threshold
Parameters used in this template:
schema = sqlgen
cdmTableName = PERSON
cdmFieldName = YEAR_OF_BIRTH
plausibleValueHigh = YEAR(GETDATE())+1
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
            'PERSON.YEAR_OF_BIRTH' AS violating_field, 
		    cdmTable.*
    	FROM {cdm_schema}.PERSON cdmTable
      	    WHERE cdmTable.YEAR_OF_BIRTH > EXTRACT(YEAR FROM CURRENT_DATE)+1
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.PERSON cdmTable
  	WHERE YEAR_OF_BIRTH IS NOT NULL
) denominator
) cte

