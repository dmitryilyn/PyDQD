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
  ,'sourceValueCompleteness' as check_name
  ,'FIELD' as check_level
  ,'The number and percent of distinct source values in the DEVICE_SOURCE_VALUE field of the DEVICE_EXPOSURE table mapped to 0.' as check_description
  ,'DEVICE_EXPOSURE' as cdm_table_name
  ,'DEVICE_SOURCE_VALUE' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_source_value_completeness.sql' as sql_file
  ,'Completeness' as category
  ,'NA' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_sourcevaluecompleteness_device_exposure_device_source_value' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,100 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
SOURCE_VALUE_COMPLETENESS
number of source values with 0 standard concept / number of distinct source values
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = DEVICE_EXPOSURE
cdmFieldName = DEVICE_SOURCE_VALUE
standardConceptFieldName = DEVICE_CONCEPT_ID
**********/
SELECT num_violated_rows,
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
		SELECT DISTINCT 
		  'DEVICE_EXPOSURE.DEVICE_SOURCE_VALUE' AS violating_field, 
		  cdmTable.DEVICE_SOURCE_VALUE
		FROM {cdm_schema}.DEVICE_EXPOSURE cdmTable
		WHERE cdmTable.DEVICE_CONCEPT_ID = 0
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT 
	  COUNT(distinct cdmTable.DEVICE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.DEVICE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
	FROM {cdm_schema}.DEVICE_EXPOSURE cdmTable
) denominator
) cte

