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
  ,'plausibleGender' as check_name
  ,'CONCEPT' as check_level
  ,'For a CONCEPT_ID  4129155 (VAGINAL BLEEDING), the number and percent of records associated with patients with an implausible gender (correct gender = FEMALE).' as check_description
  ,'CONDITION_OCCURRENCE' as cdm_table_name
  ,'CONDITION_CONCEPT_ID' as cdm_field_name
  ,' 4129155' as concept_id
  ,'NA' as unit_concept_id
  ,'concept_plausible_gender.sql' as sql_file
  ,'Plausibility' as category
  ,'Atemporal' as subcategory
  ,'Validation' as context
  ,'' as warning
  ,'' as error
  ,'concept_plausiblegender_condition_occurrence_condition_concept_id_4129155' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,5 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
CONCEPT LEVEL check:
PLAUSIBLE_GENDER - number of records of a given concept which occur in person with implausible gender for that concept
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = CONDITION_OCCURRENCE
cdmFieldName = CONDITION_CONCEPT_ID
conceptId =  4129155
plausibleGender = Female
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
	  COUNT(*) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT cdmTable.* 
		FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
			INNER JOIN {cdm_schema}.person p
			ON cdmTable.person_id = p.person_id
		WHERE cdmTable.CONDITION_CONCEPT_ID =  4129155
		  AND p.gender_concept_id <> 8532 
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.CONDITION_OCCURRENCE cdmTable
	WHERE CONDITION_CONCEPT_ID =  4129155
) denominator
) cte

