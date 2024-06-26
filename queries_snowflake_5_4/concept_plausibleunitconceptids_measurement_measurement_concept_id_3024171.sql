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
  ,'plausibleUnitConceptIds' as check_name
  ,'CONCEPT' as check_level
  ,'The number and percent of records for a given CONCEPT_ID  3024171 (RESPIRATORY RATE) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN  (8483,8483,8541,8541,8543,8543,9465,9465,9466,9466,9469,9469,9480,9480,9516,9516,9521,9521,9688,9688,9690,9690,9691,9691,9692,9692,44777521,44777521,44777554,44777554,44777556,44777556,44777557,44777557,44777658,44777658,44777659,44777659,44819154,44819154,45891007,45891007,45891008,45891008,45891031,45891031)).' as check_description
  ,'MEASUREMENT' as cdm_table_name
  ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
  ,' 3024171' as concept_id
  ,'NA' as unit_concept_id
  ,'concept_plausible_unit_concept_ids.sql' as sql_file
  ,'Plausibility' as category
  ,'Atemporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_3024171' as checkid
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
PLAUSIBLE_UNIT_CONCEPT_IDS - find any MEASUREMENT records that are associated with an incorrect UNIT_CONCEPT_ID
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = MEASUREMENT
cdmFieldName = MEASUREMENT_CONCEPT_ID
conceptId =  3024171
plausibleUnitConceptIds = 8483,8483,8541,8541,8543,8543,9465,9465,9466,9466,9469,9469,9480,9480,9516,9516,9521,9521,9688,9688,9690,9690,9691,9691,9692,9692,44777521,44777521,44777554,44777554,44777556,44777556,44777557,44777557,44777658,44777658,44777659,44777659,44819154,44819154,45891007,45891007,45891008,45891008,45891031,45891031
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
		SELECT 
		  m.* 
		FROM {cdm_schema}.MEASUREMENT m
		WHERE m.MEASUREMENT_CONCEPT_ID =  3024171
		  AND 
		  m.unit_concept_id NOT IN (8483,8483,8541,8541,8543,8543,9465,9465,9466,9466,9469,9469,9480,9480,9516,9516,9521,9521,9688,9688,9690,9690,9691,9691,9692,9692,44777521,44777521,44777554,44777554,44777556,44777556,44777557,44777557,44777658,44777658,44777659,44777659,44819154,44819154,45891007,45891007,45891008,45891008,45891031,45891031)
		  AND m.value_as_number IS NOT NULL 
		  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.MEASUREMENT m
	WHERE m.MEASUREMENT_CONCEPT_ID =  3024171
	  AND value_as_number IS NOT NULL
	  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte

