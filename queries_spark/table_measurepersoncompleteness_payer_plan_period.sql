-- check_name: measurePersonCompleteness
-- check_level: TABLE
-- checkDescription: TABLE
-- cdm_table_name: PAYER_PLAN_PERIOD
-- cdm_field_name: NA
-- concept_id: NA
-- unit_concept_id: NA
-- sql_file: table_person_completeness.sql
-- category: Completeness
-- subcategory: 
-- context: Validation
-- checkId: table_measurepersoncompleteness_payer_plan_period
-- thresholdValue: 0
SELECT 
  cte.num_violated_rows,
  cte.pct_violated_rows,
  cte.num_denominator_rows,
  CAST(NULL AS FLOAT) as execution_time,
  CAST(NULL AS STRING) as query_text,
  CAST('measurePersonCompleteness' AS STRING) as check_name,
  CAST('TABLE' AS STRING) as check_level,
  CAST('TABLE' AS STRING) as check_description,
  CAST('PAYER_PLAN_PERIOD' AS STRING) as cdm_table_name,
  CAST(NULL AS STRING) as cdm_field_name,
  CAST(NULL AS INTEGER) as concept_id,
  CAST(NULL AS INTEGER) as unit_concept_id,
  CAST('table_person_completeness.sql' AS STRING) as sql_file,
  CAST('Completeness' AS STRING) as category,
  CAST('' AS STRING) as subcategory,
  CAST('Validation' AS STRING) context,
  '' as warning,
  '' as error,
  'table_measurepersoncompleteness_payer_plan_period' as checkid,
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
 COUNT(violated_rows.person_id) AS num_violated_rows
 FROM
 (
 /*violatedRowsBegin*/
 SELECT 
 cdmTable.* 
 FROM {cdm_schema}.person cdmTable
 LEFT JOIN {cdm_schema}.PAYER_PLAN_PERIOD cdmTable2 
 ON cdmTable.person_id = cdmTable2.person_id
 WHERE cdmTable2.person_id IS NULL
 /*violatedRowsEnd*/
 ) violated_rows
) violated_row_count cross join (SELECT 
 COUNT(*) AS num_rows
 FROM {cdm_schema}.person cdmTable
) denominator
) cte

