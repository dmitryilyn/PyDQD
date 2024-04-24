-- check_name: plausibleUnitConceptIds
-- check_level: CONCEPT
-- checkDescription: CONCEPT
-- cdm_table_name: MEASUREMENT
-- cdm_field_name: MEASUREMENT_CONCEPT_ID
-- concept_id: 3016258
-- unit_concept_id: NA
-- sql_file: concept_plausible_unit_concept_ids.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: concept_plausibleunitconceptids_measurement_measurement_concept_id_3016258
-- thresholdValue: 5
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('plausibleUnitConceptIds' AS STRING) as check_name,
  CAST('CONCEPT' AS STRING) as check_level,
  CAST('CONCEPT' AS STRING) as check_description,
  CAST('MEASUREMENT' AS STRING) as cdm_table_name,
  CAST('MEASUREMENT_CONCEPT_ID' AS STRING) as cdm_field_name,
  CAST('3016258' AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('concept_plausible_unit_concept_ids.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'concept_plausibleunitconceptids_measurement_measurement_concept_id_3016258' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  5 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
CONCEPT LEVEL check:
PLAUSIBLE_UNIT_CONCEPT_IDS - find any MEASUREMENT records that are associated with an incorrect UNIT_CONCEPT_ID
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = MEASUREMENT
cdmFieldName = MEASUREMENT_CONCEPT_ID
conceptId =  3016258
plausibleUnitConceptIds = 8577,8582,8588,9279,9280,9281,9282,9290,9305,9306,9307,9308,9309,9310,9311,9321,9326,9327,9330,9349,9350,9351,9352,9355,9361,9362,9363,9364,9365,9370,9371,9375,9376,9377,9381,9384,9385,9386,9395,9396,9397,9398,9407,9419,9420,9421,9487,9497,9536,9546,9624,9629,9666,32739,32963
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
		WHERE m.MEASUREMENT_CONCEPT_ID =  3016258
		  AND 
		  m.unit_concept_id NOT IN (8577,8582,8588,9279,9280,9281,9282,9290,9305,9306,9307,9308,9309,9310,9311,9321,9326,9327,9330,9349,9350,9351,9352,9355,9361,9362,9363,9364,9365,9370,9371,9375,9376,9377,9381,9384,9385,9386,9395,9396,9397,9398,9407,9419,9420,9421,9487,9497,9536,9546,9624,9629,9666,32739,32963)
		  AND m.value_as_number IS NOT NULL 
		  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.MEASUREMENT m
	WHERE m.MEASUREMENT_CONCEPT_ID =  3016258
	  AND value_as_number IS NOT NULL
	  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator

) cte

