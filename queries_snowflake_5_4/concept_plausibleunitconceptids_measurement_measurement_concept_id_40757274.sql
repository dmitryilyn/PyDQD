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
  ,'The number and percent of records for a given CONCEPT_ID 40757274 (GARDNERELLA VAGINALIS DNA [PRESENCE] IN VAGINAL FLUID BY PROBE WITH SIGNAL AMPLIFICATION) with implausible units (i.e., UNIT_CONCEPT_ID NOT IN  (NA)).' as check_description
  ,'MEASUREMENT' as cdm_table_name
  ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
  ,'40757274' as concept_id
  ,'NA' as unit_concept_id
  ,'concept_plausible_unit_concept_ids.sql' as sql_file
  ,'Plausibility' as category
  ,'Atemporal' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'concept_plausibleunitconceptids_measurement_measurement_concept_id_40757274' as checkid
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
conceptId = 40757274
plausibleUnitConceptIds = NA
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
		WHERE m.MEASUREMENT_CONCEPT_ID = 40757274
		  AND 
		  m.unit_concept_id IS NOT NULL
		  AND m.value_as_number IS NOT NULL 
		  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.MEASUREMENT m
	WHERE m.MEASUREMENT_CONCEPT_ID = 40757274
	  AND value_as_number IS NOT NULL
	  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator
) cte

