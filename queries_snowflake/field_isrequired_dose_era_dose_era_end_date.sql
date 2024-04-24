-- check_name: isRequired
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DOSE_ERA
-- cdm_field_name: DOSE_ERA_END_DATE
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_is_not_nullable.sql
-- category: Conformance
-- subcategory: Relational
-- context: Validation
-- checkId: field_isrequired_dose_era_dose_era_end_date
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('isRequired' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('DOSE_ERA' AS STRING) as cdm_table_name,
  CAST('DOSE_ERA_END_DATE' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_is_not_nullable.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Relational' AS STRING) as subcategory,
  CAST('Validation' AS STRING) context,
  '' as warning,
  '' as error,
  'field_isrequired_dose_era_dose_era_end_date' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
FIELD_IS_NOT_NULLABLE
For each table, check that the fields in which IS_NOT_NULLABLE == TRUE, there are no null values in that field.
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = DOSE_ERA
cdmFieldName = DOSE_ERA_END_DATE
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
		  'DOSE_ERA.DOSE_ERA_END_DATE' AS violating_field, 
		  cdmTable.* 
		FROM {cdm_schema}.DOSE_ERA cdmTable
		WHERE cdmTable.DOSE_ERA_END_DATE IS NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.DOSE_ERA cdmTable
) denominator

) cte
