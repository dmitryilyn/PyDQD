
-- check_name: standardConceptRecordCompleteness
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: FACT_RELATIONSHIP
-- cdm_field_name: RELATIONSHIP_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_concept_record_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Verification
-- checkId: field_standardconceptrecordcompleteness_fact_relationship_relationship_concept_id
-- thresholdValue: 100
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'standardConceptRecordCompleteness' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'FACT_RELATIONSHIP' as cdm_table_name,
  'RELATIONSHIP_CONCEPT_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_concept_record_completeness.sql' as sql_file,
  'Completeness' as category,
  '' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_standardconceptrecordcompleteness_fact_relationship_relationship_concept_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  100 as threshold_value,
  NULL as notes_value
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
 'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.FACT_RELATIONSHIP cdmTable
 WHERE cdmTable.RELATIONSHIP_CONCEPT_ID = 0 
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT COUNT(*) AS num_rows
 FROM {cdm_schema}.FACT_RELATIONSHIP cdmTable
) denominator
) cte

