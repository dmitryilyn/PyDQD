
-- check_name: fkDomain
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: VISIT_DETAIL
-- cdm_field_name: VISIT_DETAIL_TYPE_CONCEPT_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_fk_domain.sql
-- category: Conformance
-- subcategory: Value
-- context: Verification
-- checkId: field_fkdomain_visit_detail_visit_detail_type_concept_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'fkDomain' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'VISIT_DETAIL' as cdm_table_name,
  'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_fk_domain.sql' as sql_file,
  'Conformance' as category,
  'Value' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_fkdomain_visit_detail_visit_detail_type_concept_id' as checkid,
  0 as is_error,
  0 as not_applicable,
  CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed,
  CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed,
  NULL as not_applicable_reason,
  0 as threshold_value,
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
 'VISIT_DETAIL.VISIT_DETAIL_TYPE_CONCEPT_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.VISIT_DETAIL cdmTable
 LEFT JOIN {cdm_schema}.concept co
 ON cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID = co.concept_id
 WHERE co.concept_id != 0 
 AND co.domain_id NOT IN ('Type Concept')
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.VISIT_DETAIL cdmTable
) denominator
) cte

