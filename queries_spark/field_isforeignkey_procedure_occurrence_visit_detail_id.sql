
-- check_name: isForeignKey
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PROCEDURE_OCCURRENCE
-- cdm_field_name: VISIT_DETAIL_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: is_foreign_key.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_isforeignkey_procedure_occurrence_visit_detail_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'isForeignKey' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'PROCEDURE_OCCURRENCE' as cdm_table_name,
  'VISIT_DETAIL_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'is_foreign_key.sql' as sql_file,
  'Conformance' as category,
  'Relational' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_isforeignkey_procedure_occurrence_visit_detail_id' as checkid,
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
 'PROCEDURE_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field, 
 cdmTable.*
 FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
 LEFT JOIN {cdm_schema}.VISIT_DETAIL fkTable
 ON cdmTable.VISIT_DETAIL_ID = fkTable.VISIT_DETAIL_ID
 WHERE fkTable.VISIT_DETAIL_ID IS NULL 
 AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.PROCEDURE_OCCURRENCE cdmTable
) denominator
) cte

