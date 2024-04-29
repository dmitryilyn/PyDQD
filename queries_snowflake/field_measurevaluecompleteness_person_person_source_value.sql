-- check_name: measureValueCompleteness
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PERSON
-- cdm_field_name: PERSON_SOURCE_VALUE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_measure_value_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Verification
-- checkId: field_measurevaluecompleteness_person_person_source_value
-- thresholdValue: 100
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('measureValueCompleteness' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('PERSON' AS STRING) as cdm_table_name,
  CAST('PERSON_SOURCE_VALUE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_measure_value_completeness.sql' AS STRING) as sql_file,
  CAST('Completeness' AS STRING) as category,
  CAST('' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_measurevaluecompleteness_person_person_source_value' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  100 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
MEASURE_VALUE_COMPLETENESS
Computing number of null values and the proportion to total records per field
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = PERSON
cdmFieldName = PERSON_SOURCE_VALUE
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
			'PERSON.PERSON_SOURCE_VALUE' AS violating_field, 
			cdmTable.* 
		FROM {cdm_schema}.PERSON cdmTable
		WHERE cdmTable.PERSON_SOURCE_VALUE IS NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.PERSON cdmTable
) denominator


) cte

