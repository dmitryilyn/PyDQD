
-- check_name: isPrimaryKey
-- check_level: FIELD
-- checkDescription: FIELD
-- cdm_table_name: PAYER_PLAN_PERIOD
-- cdm_field_name: PAYER_PLAN_PERIOD_ID
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: field_is_primary_key.sql
-- category: Conformance
-- subcategory: Relational
-- context: Verification
-- checkId: field_isprimarykey_payer_plan_period_payer_plan_period_id
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  '' as execution_time,
  '' as query_text,
  'isPrimaryKey' as check_name,
  'FIELD' as check_level,
  'FIELD' as check_description,
  'PAYER_PLAN_PERIOD' as cdm_table_name,
  'PAYER_PLAN_PERIOD_ID' as cdm_field_name,
  'NA' as concept_id,
  'NA' as unit_concept_id,
  'field_is_primary_key.sql' as sql_file,
  'Conformance' as category,
  'Relational' as subcategory,
  'Verification' as context,
  '' as warning,
  '' as error,
  'field_isprimarykey_payer_plan_period_payer_plan_period_id' as checkid,
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
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_ID' AS violating_field, 
 cdmTable.* 
 FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
 WHERE cdmTable.PAYER_PLAN_PERIOD_ID IN ( 
 SELECT 
 PAYER_PLAN_PERIOD_ID 
 FROM {cdm_schema}.PAYER_PLAN_PERIOD
 GROUP BY PAYER_PLAN_PERIOD_ID
 HAVING COUNT(*) > 1 
 )
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.PAYER_PLAN_PERIOD cdmTable
) denominator
) cte

