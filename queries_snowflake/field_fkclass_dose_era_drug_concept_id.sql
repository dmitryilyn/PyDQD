-- check_name: fkClass
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: DOSE_ERA
-- cdm_field_name: DRUG_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_fk_class.sql
-- category: Conformance
-- subcategory: Computational
-- context: Verification
-- checkId: field_fkclass_dose_era_drug_concept_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('fkClass' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('DOSE_ERA' AS STRING) as cdm_table_name,
  CAST('DRUG_CONCEPT_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_fk_class.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Computational' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_fkclass_dose_era_drug_concept_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  /*********
FK_CLASS
Drug era standard concepts, ingredients only
Parameters used in this template:
cdmDatabaseSchema = sqlgen
vocabDatabaseSchema = sqlgen
cdmTableName = DOSE_ERA
cdmFieldName = DRUG_CONCEPT_ID
fkClass = Ingredient
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
		  'DOSE_ERA.DRUG_CONCEPT_ID' AS violating_field, 
		  cdmTable.* 
		FROM {cdm_schema}.DOSE_ERA cdmTable
		  LEFT JOIN {cdm_schema}.concept co
		  ON cdmTable.DRUG_CONCEPT_ID = co.concept_id
    WHERE co.concept_id != 0 
      AND (co.concept_class_id != 'Ingredient') 
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT(*) AS num_rows
	FROM {cdm_schema}.DOSE_ERA cdmTable
) denominator


) cte

