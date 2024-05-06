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
  ,'cdmDatatype' as check_name
  ,'FIELD' as check_level
  ,'A yes or no value indicating if the RELATIONSHIP_CONCEPT_ID in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
  ,'FACT_RELATIONSHIP' as cdm_table_name
  ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
  ,'NA' as concept_id
  ,'NA' as unit_concept_id
  ,'field_cdm_datatype.sql' as sql_file
  ,'Conformance' as category
  ,'Value' as subcategory
  ,'Verification' as context
  ,'' as warning
  ,'' as error
  ,'field_cdmdatatype_fact_relationship_relationship_concept_id' as checkid
  ,0 as is_error
  ,0 as not_applicable
  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
  ,NULL as not_applicable_reason
  ,0 as threshold_value
  ,NULL as notes_value
FROM (
  /*********
FIELD_CDM_DATATYPE
In some SQL dialects, check that integer fields only contain digits.
In others, check that integer fields are numeric and contain no decimal points
Parameters used in this template:
schema = sqlgen
cdmTableName = FACT_RELATIONSHIP
cdmFieldName = RELATIONSHIP_CONCEPT_ID
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
			'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field, 
			cdmTable.* 
		FROM {cdm_schema}.FACT_RELATIONSHIP cdmTable
		WHERE 
			(IS_REAL(TRY_TO_NUMERIC(cdmTable.RELATIONSHIP_CONCEPT_ID)) = 0 
			OR (IS_REAL(TRY_TO_NUMERIC(cdmTable.RELATIONSHIP_CONCEPT_ID)) = 1 
				AND CHARINDEX('.', CAST(ABS(cdmTable.RELATIONSHIP_CONCEPT_ID) AS varchar)) != 0))
			AND cdmTable.RELATIONSHIP_CONCEPT_ID IS NOT NULL
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
		COUNT(*) AS num_rows
	FROM {cdm_schema}.FACT_RELATIONSHIP
) denominator
) cte

