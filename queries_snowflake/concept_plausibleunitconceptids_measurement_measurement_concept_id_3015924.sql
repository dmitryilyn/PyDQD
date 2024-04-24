-- check_name: plausibleUnitConceptIds
-- check_level: CONCEPT
-- checkDescription: CONCEPT
-- cdm_table_name: MEASUREMENT
-- cdm_field_name: MEASUREMENT_CONCEPT_ID
-- concept_id: 3015924
-- unit_concept_id: NA
-- sql_file: concept_plausible_unit_concept_ids.sql
-- category: Plausibility
-- subcategory: Atemporal
-- context: Verification
-- checkId: concept_plausibleunitconceptids_measurement_measurement_concept_id_3015924
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
  CAST('3015924' AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('concept_plausible_unit_concept_ids.sql' AS STRING) as sql_file,
  CAST('Plausibility' AS STRING) as category,
  CAST('Atemporal' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'concept_plausibleunitconceptids_measurement_measurement_concept_id_3015924' as checkid,
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
conceptId =  3015924
plausibleUnitConceptIds = 8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655
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
		WHERE m.MEASUREMENT_CONCEPT_ID =  3015924
		  AND 
		  m.unit_concept_id NOT IN (8504,8564,8576,8739,9275,9294,9295,9319,9343,9345,9347,9354,9356,9357,9372,9373,9374,9392,9400,9402,9409,9410,9425,9479,9485,9496,9529,9600,9647,9648,9655)
		  AND m.value_as_number IS NOT NULL 
		  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.MEASUREMENT m
	WHERE m.MEASUREMENT_CONCEPT_ID =  3015924
	  AND value_as_number IS NOT NULL
	  AND (m.unit_source_value IS NOT NULL OR m.unit_source_value <> '')
) denominator

) cte

