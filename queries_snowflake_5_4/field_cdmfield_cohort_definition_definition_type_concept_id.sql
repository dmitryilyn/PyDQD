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
  ,'cdmField' as check_name
  ,'FIELD' as check_level
  ,'A yes or no value indicating if DEFINITION_TYPE_CONCEPT_ID is present in the COHORT_DEFINITION table as expected based on the specification.' as check_description
  ,'COHORT_DEFINITION' as cdm_table_name
  ,'DEFINITION_TYPE_CONCEPT_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_cdm_field.sql' as sql_file
  ,'Conformance' as category
  ,'Relational' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_cdmfield_cohort_definition_definition_type_concept_id' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD LEVEL check:
CDM_FIELD - verify the field exists
Parameters used in this template:
cdmDatabaseSchema = sqlgen
cdmTableName = COHORT_DEFINITION
cdmFieldName = DEFINITION_TYPE_CONCEPT_ID
**********/
SELECT 
  num_violated_rows, 
  CASE 
    WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows 
  END AS pct_violated_rows, 
  denominator.num_rows AS num_denominator_rows
FROM (
  SELECT num_violated_rows FROM (
    SELECT
      CASE 
        WHEN COUNT(DEFINITION_TYPE_CONCEPT_ID) = 0 THEN 0
        ELSE 0
      END AS num_violated_rows
    FROM {cdm_schema}.COHORT_DEFINITION cdmTable
    ) violated_rows
) violated_row_count,
( 
  SELECT 1 AS num_rows
) denominator
) cte

