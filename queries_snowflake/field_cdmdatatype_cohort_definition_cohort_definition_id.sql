-- check_name: cdmDatatype
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: COHORT_DEFINITION
-- cdm_field_name: COHORT_DEFINITION_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_cdm_datatype.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_cdmdatatype_cohort_definition_cohort_definition_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('cdmDatatype' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('COHORT_DEFINITION' AS STRING) as cdm_table_name,
  CAST('COHORT_DEFINITION_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_cdm_datatype.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Value' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_cdmdatatype_cohort_definition_cohort_definition_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
FIELD_CDM_DATATYPE
At a minimum, for each field that is supposed to be an integer, verify it is an integer
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = COHORT_DEFINITION
cdmFieldName = COHORT_DEFINITION_ID
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
		  'COHORT_DEFINITION.COHORT_DEFINITION_ID' AS violating_field, 
		  cdmTable.* 
		FROM {cdm_schema}.COHORT_DEFINITION cdmTable
		WHERE IS_REAL(TRY_TO_NUMERIC(abs(cdmTable.COHORT_DEFINITION_ID))) = 0 
		  AND cdmTable.COHORT_DEFINITION_ID IS NOT NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.COHORT_DEFINITION
) denominator

) cte

