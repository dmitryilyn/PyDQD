-- check_name: fkDomain
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: SPECIMEN
-- cdm_field_name: SPECIMEN_TYPE_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_fk_domain.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_fkdomain_specimen_specimen_type_concept_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('fkDomain' AS STRING) as check_name,
  CAST('FIELD' AS STRING) as check_level,
  CAST('FIELD' AS STRING) as check_description,
  CAST('SPECIMEN' AS STRING) as cdm_table_name,
  CAST('SPECIMEN_TYPE_CONCEPT_ID' AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('field_fk_domain.sql' AS STRING) as sql_file,
  CAST('Conformance' AS STRING) as category,
  CAST('Value' AS STRING) as subcategory,
  CAST('Verification' AS STRING) context,
  '' as warning,
  '' as error,
  'field_fkdomain_specimen_specimen_type_concept_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  CAST(NULL AS STRING) as not_applicable_reason,
  0 as threshold_value,
  CAST(NULL AS STRING) as notes_value
FROM (
  SELECT num_violated_rows, 
 CASE
 WHEN denominator.num_rows = 0 THEN 0 
 ELSE 1.0*num_violated_rows/denominator.num_rows 
 END AS pct_violated_rows, 
 denominator.num_rows AS num_denominator_rows
 FROM (SELECT 
 COUNT(violated_rows.violating_field) AS num_violated_rows
 FROM (
 /*violatedRowsBegin*/
 SELECT 
 'SPECIMEN.SPECIMEN_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.SPECIMEN cdmTable
 LEFT JOIN {cdm_schema}.concept co
 ON cdmTable.SPECIMEN_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND co.domain_id NOT IN ('Type Concept')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.SPECIMEN cdmTable
) denominator
) cte

